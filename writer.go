package goyxdb

import "io"

type YxdbWriter interface {
	Write(record RecordBlob) error
	io.Closer
}

type yxdbWriter struct {
}

func (w *yxdbWriter) Write(record RecordBlob) error {
	return nil
}

func (w *yxdbWriter) Close() error {
	return nil
}
