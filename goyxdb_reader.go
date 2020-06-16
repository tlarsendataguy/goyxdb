package goyxdb

import (
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

type YxdbReader interface {
	RecordInfoXml() string
	Next() bool
	Error() error
	Record() unsafe.Pointer
	io.Closer
}

type yxdbReader struct {
	recordInfoXml string
	header        []byte
	file          *os.File
	err           error
	fixedSize     uint32
	inBuffer      []byte
	outBuffer     []byte
	outBufferSize uint32
	currentPos    uint32
	currentVarLen uint32
	currentRecord int
}

const bufferSize uint32 = 0x40000

type recordInfoXml struct {
	XMLName xml.Name `xml:"RecordInfo"`
	Field   []fieldXml
}

type fieldXml struct {
	Type string `xml:"type,attr"`
	Size uint32 `xml:"size,attr"`
}

func (yxdb *yxdbReader) fileDesc() string {
	return string(yxdb.header[0:64])
}

func (yxdb *yxdbReader) fileId() uint32 {
	return binary.LittleEndian.Uint32(yxdb.header[64:68])
}

func (yxdb *yxdbReader) createdAt() uint32 {
	return binary.LittleEndian.Uint32(yxdb.header[68:72])
}

func (yxdb *yxdbReader) metaInfoSize() uint32 {
	return binary.LittleEndian.Uint32(yxdb.header[80:84])
}

func (yxdb *yxdbReader) recordBlockIndexPos() int {
	return int(binary.LittleEndian.Uint64(yxdb.header[96:104]))
}

func (yxdb *yxdbReader) numRecords() int {
	return int(binary.LittleEndian.Uint64(yxdb.header[104:112]))
}

func (yxdb *yxdbReader) compressionVersion() uint32 {
	return binary.LittleEndian.Uint32(yxdb.header[112:116])
}

func (yxdb *yxdbReader) RecordInfoXml() string {
	return yxdb.recordInfoXml
}

func (yxdb *yxdbReader) Next() bool {
	yxdb.currentRecord++
	if yxdb.currentRecord > yxdb.numRecords() {
		return false
	}

	yxdb.currentPos += yxdb.fixedSize + 4 + yxdb.currentVarLen

	if yxdb.currentPos > yxdb.outBufferSize {
		yxdb.currentPos -= yxdb.fixedSize + 4 + yxdb.currentVarLen
		ok, err := yxdb.loadNextBuffer()
		yxdb.err = err
		if err != nil || !ok {
			return false
		}
	}

	varLenIntPos := yxdb.currentPos + yxdb.fixedSize
	yxdb.currentVarLen = binary.LittleEndian.Uint32(yxdb.outBuffer[varLenIntPos : varLenIntPos+4])

	return true
}

func (yxdb *yxdbReader) Error() error {
	return yxdb.err
}

func (yxdb *yxdbReader) Record() unsafe.Pointer {
	return unsafe.Pointer(&yxdb.outBuffer[yxdb.currentPos])
}

func (yxdb *yxdbReader) Close() error {
	return yxdb.file.Close()
}

func LoadYxdbReader(path string) (YxdbReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf(`error loading YXDB: %v`, err.Error())
	}

	const headerSize = 512
	header := make([]byte, headerSize)
	_, err = file.Read(header)
	if err != nil {
		return nil, fmt.Errorf(`error reading header bytes: %v`, err.Error())
	}

	yxdb := &yxdbReader{
		header: header,
		file:   file,
	}
	metaInfoSize := yxdb.metaInfoSize()

	metaInfoBytes := make([]byte, metaInfoSize*2)
	_, err = file.Read(metaInfoBytes)
	if err != nil {
		return nil, fmt.Errorf(`error reading metainfo bytes: %v`, err.Error())
	}

	metaInfoUint16 := make([]uint16, metaInfoSize)
	for i := uint32(0); i < metaInfoSize; i++ {
		metaInfoUint16[i] = binary.LittleEndian.Uint16(metaInfoBytes[i*2 : i*2+2])
	}
	metaInfo := syscall.UTF16ToString(metaInfoUint16)
	yxdb.recordInfoXml = metaInfo

	var metaInfoXml recordInfoXml
	err = xml.Unmarshal([]byte(metaInfo), &metaInfoXml)
	if err != nil {
		return nil, err
	}
	for _, field := range metaInfoXml.Field {
		switch field.Type {
		case `Bool`:
			yxdb.fixedSize += 1
		case `Byte`:
			yxdb.fixedSize += 2
		case `Int16`:
			yxdb.fixedSize += 3
		case `Int32`:
			yxdb.fixedSize += 5
		case `Int64`:
			yxdb.fixedSize += 9
		case `String`:
			yxdb.fixedSize += field.Size + 1
		case `WString`:
			yxdb.fixedSize += field.Size*2 + 1
		case `V_String`, `V_WString`, `SpatialObj`, `Blob`:
			yxdb.fixedSize += 4
		case `Date`:
			yxdb.fixedSize += 11
		case `DateTime`:
			yxdb.fixedSize += 20
		}
	}

	yxdb.inBuffer = make([]byte, bufferSize)
	yxdb.outBuffer = make([]byte, bufferSize*2)

	return yxdb, nil
}

func (yxdb *yxdbReader) loadNextBuffer() (bool, error) {
	var delta uint32 = 0
	if yxdb.currentPos < yxdb.outBufferSize {
		delta = yxdb.outBufferSize - yxdb.currentPos
		copy(yxdb.outBuffer[0:delta], yxdb.outBuffer[yxdb.currentPos:yxdb.outBufferSize])
	}

	buffer := make([]byte, 4)
	read, err := yxdb.file.Read(buffer)
	if err != nil || read != 4 {
		return false, err
	}

	length := binary.LittleEndian.Uint32(buffer)

	println(fmt.Sprintf(`length: %v`, length))

	if length&0x80000000 > 0 { // data in block is not compressed and can be copied directly to the out buffer
		length &= 0x7fffffff
		println(`not compressed`)

		read, err = yxdb.file.Read(yxdb.outBuffer[delta : delta+length])
		if err != nil {
			return false, err
		}
		yxdb.outBufferSize = length

	} else { // data in block is compressed and needs to be decompressed
		read, err = yxdb.file.Read(yxdb.inBuffer[0:length])
		if err != nil {
			return false, err
		}

		outBufferSize, err := decompress(yxdb.inBuffer, length, yxdb.outBuffer[delta:], bufferSize*2-delta)
		if err != nil {
			println(err.Error())
			return false, err
		}
		yxdb.outBufferSize = outBufferSize + delta
	}

	if yxdb.outBufferSize == 0 {
		return false, nil
	}

	println(fmt.Sprintf(`%v`, yxdb.outBuffer[0:200]))
	yxdb.currentPos = 0
	return true, nil
}
