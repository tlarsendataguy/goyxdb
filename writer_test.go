package goyxdb_test

import (
	"github.com/tlarsen7572/goyxdb"
	"testing"
)

func TestWriteYxdb(t *testing.T) {
	writer, err := goyxdb.LoadYxdbWriter(`test.yxdb`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
}
