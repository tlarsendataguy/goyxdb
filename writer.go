package goyxdb

import (
	"io"
	"os"
)

type YxdbWriter interface {
	Write(record RecordBlob) error
	io.Closer
}

func LoadYxdbWriter(path string) (YxdbWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &yxdbWriter{file: file}, nil
}

type yxdbWriter struct {
	file *os.File
}

func (w *yxdbWriter) Write(record RecordBlob) error {
	return nil
}

func (w *yxdbWriter) Close() error {
	return w.file.Close()
}
