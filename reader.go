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
	Record() RecordBlob
	io.Closer
}

type yxdbReader struct {
	recordInfoXml    string
	header           []byte
	file             *os.File
	err              error
	fixedSize        uint32
	inBuffer1        []byte
	inBuffer2        []byte
	currentInBuffer  int
	outBuffer        []byte
	outBufferSize    uint32
	currentPos       uint32
	currentVarLen    uint32
	varLenSize       uint32
	currentRecord    int
	longRecordBuffer []byte
	isLongRecord     bool
	bufferChan       chan []byte
	lengthChan       chan uint32
	isCompressedChan chan bool
}

const bufferSize uint32 = 0x40000

type metaInfoXml struct {
	XMLName    xml.Name `xml:"MetaInfo"`
	RecordInfo *recordInfoXml
}

type recordInfoXml struct {
	XMLName xml.Name `xml:"RecordInfo"`
	Field   []fieldXml
}

type fieldXml struct {
	Name   string `xml:"name,attr"`
	Source string `xml:"source,attr"`
	Size   uint32 `xml:"size,attr"`
	Scale  uint32 `xml:"scale,attr"`
	Type   string `xml:"type,attr"`
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

	// If the prior record is a long record, we simply reset isLongRecord and don't move the current position
	// because the prior record was pointing to longRecordBuffer and not outBuffer.  If the prior record is a short
	// record then we need to move currentPos to the next record's starting point.  We also skip moving currentPos
	// if we are on the first record because currentPos is already set properly to 0.
	if yxdb.isLongRecord || yxdb.currentRecord == 1 {
		yxdb.isLongRecord = false
	} else {
		yxdb.currentPos += yxdb.fixedSize + yxdb.varLenSize + yxdb.currentVarLen
	}

	// load the next round of bytes from the file if we don't have enough bytes to grab the variable width len of
	// the current record
	if yxdb.currentPos+yxdb.fixedSize+yxdb.varLenSize > yxdb.outBufferSize {
		ok, err := yxdb.nextBuffer()
		if err != nil || !ok {
			yxdb.err = err
			return false
		}
	}

	yxdb.updateCurrentVarLen()
	recordSize := yxdb.fixedSize + yxdb.varLenSize + yxdb.currentVarLen

	if recordSize > bufferSize {
		return yxdb.processLongRecord(recordSize)
	}

	if yxdb.currentPos+recordSize > yxdb.outBufferSize {
		ok, err := yxdb.nextBuffer()
		if err != nil || !ok {
			yxdb.err = err
			return false
		}
	}

	return true
}

func (yxdb *yxdbReader) processLongRecord(recordSize uint32) bool {
	yxdb.isLongRecord = true
	if int(recordSize) > len(yxdb.longRecordBuffer) {
		yxdb.longRecordBuffer = make([]byte, recordSize)
	}

	bytesCopied := uint32(copy(yxdb.longRecordBuffer, yxdb.outBuffer[yxdb.currentPos:yxdb.outBufferSize]))
	longRecordBufferPos := bytesCopied
	yxdb.currentPos += bytesCopied
	for longRecordBufferPos < recordSize {
		ok, err := yxdb.nextBuffer()
		if err != nil || !ok {
			return false
		}

		longRecordBytesLeft := recordSize - longRecordBufferPos
		longRecordBytesToRead := longRecordBytesLeft
		if yxdb.outBufferSize < longRecordBytesToRead {
			longRecordBytesToRead = yxdb.outBufferSize
		}

		bytesCopied = uint32(copy(yxdb.longRecordBuffer[longRecordBufferPos:], yxdb.outBuffer[:longRecordBytesToRead]))
		yxdb.currentPos = bytesCopied
		longRecordBufferPos += bytesCopied
	}
	return true
}

func (yxdb *yxdbReader) updateCurrentVarLen() {
	if yxdb.varLenSize == 0 {
		return
	}
	varLenIntPos := yxdb.currentPos + yxdb.fixedSize
	yxdb.currentVarLen = binary.LittleEndian.Uint32(yxdb.outBuffer[varLenIntPos : varLenIntPos+4])
}

func (yxdb *yxdbReader) Error() error {
	return yxdb.err
}

func (yxdb *yxdbReader) Record() RecordBlob {
	if yxdb.isLongRecord {
		return NewRecordBlob(unsafe.Pointer(&yxdb.longRecordBuffer[0]))
	}
	return NewRecordBlob(unsafe.Pointer(&yxdb.outBuffer[yxdb.currentPos]))
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
	metaInfoStr := syscall.UTF16ToString(metaInfoUint16)

	var metaInfo metaInfoXml
	err = xml.Unmarshal([]byte(metaInfoStr), &metaInfo)
	if err != nil {
		// Some (older?) YXDBs do not have the surrounding MetaInfo tags.  If unmarshal has an error, it is probably
		// because MetaInfo is missing.  Add the tag manually and try again.
		metaInfoStr = `<MetaInfo>` + metaInfoStr + `</MetaInfo>`
		err = xml.Unmarshal([]byte(metaInfoStr), &metaInfo)
		if err != nil {
			return nil, err
		}
	}
	yxdb.recordInfoXml = metaInfoStr

	varFields := 0
	for _, field := range metaInfo.RecordInfo.Field {
		switch field.Type {
		case `Bool`:
			yxdb.fixedSize += 1
		case `Byte`:
			yxdb.fixedSize += 2
		case `Int16`:
			yxdb.fixedSize += 3
		case `Int32`, `Float`:
			yxdb.fixedSize += 5
		case `Int64`, `Double`:
			yxdb.fixedSize += 9
		case `String`, `FixedDecimal`:
			yxdb.fixedSize += field.Size + 1
		case `WString`:
			yxdb.fixedSize += field.Size*2 + 1
		case `V_String`, `V_WString`, `SpatialObj`, `Blob`:
			varFields++
			yxdb.fixedSize += 4
		case `Date`:
			yxdb.fixedSize += 11
		case `DateTime`:
			yxdb.fixedSize += 20
		}
	}

	yxdb.inBuffer1 = make([]byte, bufferSize)
	yxdb.inBuffer2 = make([]byte, bufferSize)
	yxdb.currentInBuffer = 1
	yxdb.bufferChan = make(chan []byte)
	yxdb.lengthChan = make(chan uint32)
	yxdb.isCompressedChan = make(chan bool)
	yxdb.outBuffer = make([]byte, bufferSize*2)
	if varFields == 0 {
		yxdb.varLenSize = 0
	} else {
		yxdb.varLenSize = 4
	}
	go yxdb.readBufferLoop()
	return yxdb, nil
}

func (yxdb *yxdbReader) nextBuffer() (bool, error) {
	inBuffer, more := <-yxdb.bufferChan
	if !more {
		return false, nil
	}
	length := <-yxdb.lengthChan
	isCompressed := <-yxdb.isCompressedChan

	var delta uint32 = 0
	if yxdb.currentPos < yxdb.outBufferSize {
		delta = yxdb.outBufferSize - yxdb.currentPos
		copy(yxdb.outBuffer[0:delta], yxdb.outBuffer[yxdb.currentPos:yxdb.outBufferSize])
	}

	if isCompressed {
		outBufferSize, err := decompress(inBuffer, length, yxdb.outBuffer[delta:], bufferSize*2-delta)
		if err != nil {
			return false, nil
		}
		yxdb.outBufferSize = outBufferSize + delta
	} else {
		copy(yxdb.outBuffer[delta:], inBuffer[:length])
		yxdb.outBufferSize = length + delta
	}

	yxdb.currentPos = 0
	return true, nil
}

func (yxdb *yxdbReader) readBufferLoop() {
	for {
		var inBuffer []byte
		if yxdb.currentInBuffer == 1 {
			inBuffer = yxdb.inBuffer1
			yxdb.currentInBuffer = 2
		} else {
			inBuffer = yxdb.inBuffer2
			yxdb.currentInBuffer = 1
		}

		buffer := make([]byte, 4)
		read, err := yxdb.file.Read(buffer)
		if err != nil || read != 4 {
			yxdb.err = err
			yxdb.closeChannels()
			return
		}

		length := binary.LittleEndian.Uint32(buffer)

		var isCompressed bool
		if length&0x80000000 > 0 { // data in block is not compressed
			isCompressed = false
			length &= 0x7fffffff

		} else { // data in block is compressed and needs to be decompressed
			isCompressed = true
		}

		read, err = yxdb.file.Read(inBuffer[:length])
		if err != nil {
			yxdb.err = err
			yxdb.closeChannels()
			return
		}

		if read == 0 {
			yxdb.closeChannels()
			return
		}

		yxdb.bufferChan <- inBuffer
		yxdb.lengthChan <- length
		yxdb.isCompressedChan <- isCompressed
	}
}

func (yxdb *yxdbReader) closeChannels() {
	close(yxdb.bufferChan)
	close(yxdb.lengthChan)
	close(yxdb.isCompressedChan)
}
