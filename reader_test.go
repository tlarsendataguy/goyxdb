package goyxdb_test

import (
	"github.com/tlarsen7572/goyxdb"
	"testing"
)

func TestLoadYxdb(t *testing.T) {
	yxdb, err := goyxdb.LoadYxdbReader(`TutorialData.yxdb`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	if yxdb.RecordInfoXml() == `` {
		t.Fatalf(`expected a record info but got none`)
	}

	if yxdb.RecordInfoXml() != expectedMetaInfo {
		t.Fatalf("expected\n%v\nbut got\n%v", expectedMetaInfo, yxdb.RecordInfoXml())
	}

	recordCount := 0
	expectedId := 100
	for yxdb.Next() {
		actualId := getUserIdFromRecordBlob(yxdb.Record())
		if actualId != expectedId {
			t.Fatalf(`expected id %v but got %v`, expectedId, actualId)
		}
		recordCount++
		expectedId++
	}

	err = yxdb.Close()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	if recordCount != 8716 {
		t.Fatalf(`expected 8,716 records but got %v`, recordCount)
	}
}

func TestLoadYxdbWithLargeField(t *testing.T) {
	yxdb, err := goyxdb.LoadYxdbReader(`VeryLongField.yxdb`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	recordCount := 0
	var expectedId byte = 1
	for yxdb.Next() {
		actualId := *((*byte)(yxdb.Record().Blob()))
		if actualId != expectedId {
			t.Fatalf(`expected id %v but got %v`, expectedId, actualId)
		}
		recordCount++
		expectedId++
	}

	err = yxdb.Close()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	if recordCount != 3 {
		t.Fatalf(`expected 3 records but got %v`, recordCount)
	}
}

func TestReadLotsOfRecords(t *testing.T) {
	yxdb, err := goyxdb.LoadYxdbReader(`LotsOfRecords.yxdb`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	recordCount := 0
	var expectedId uint32 = 1
	for yxdb.Next() {
		actualId := *((*uint32)(yxdb.Record().Blob()))
		if actualId != expectedId {
			t.Fatalf(`expected id %v but got %v`, expectedId, actualId)
		}
		recordCount++
		expectedId++
	}

	err = yxdb.Close()
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	if recordCount != 100000 {
		t.Fatalf(`expected 100,000 records but got %v`, recordCount)
	}
}

func TestNewYxdbMetaInfo(t *testing.T) {
	yxdb, err := goyxdb.LoadYxdbReader(`TestNewYxdb.yxdb`)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}

	if expectedNewMetaInfo != yxdb.RecordInfoXml() {
		t.Fatalf("expected\n%v\nbut got\n%v", expectedNewMetaInfo, yxdb.RecordInfoXml())
	}
}

const expectedMetaInfo = `<MetaInfo><RecordInfo>
	<Field name="UserID" source="RecordID: Starting Value=100" type="Int32"/>
	<Field name="First" size="12" source="Formula: titlecase([_CurrentField_])" type="V_WString"/>
	<Field name="Last" size="19" source="Formula: titlecase([_CurrentField_])" type="V_WString"/>
	<Field name="Prefix" size="12" source="Formula: titlecase([_CurrentField_])" type="V_String"/>
	<Field name="Gender" size="16" source="Formula: Replace([Gender], &quot;*~~//*~~//femal&quot;, &quot;*~~//*~~//female&quot;)" type="String"/>
	<Field name="Birth Date" source="DateTime: To yyyy-MM-dd hh:mm:ss" type="DateTime"/>
	<Field name="Registration Date/Time" source="DateTime: To yyyy-MM-dd hh:mm:ss" type="DateTime"/>
	<Field name="Email" size="35" source="CrossTab:Header:JSON_Name:email:Concat:" type="V_WString"/>
	<Field name="Country" size="2" source="CrossTab:Header:JSON_Name:nat:Concat:" type="String"/>
</RecordInfo>
</MetaInfo>`

const expectedNewMetaInfo = `<MetaInfo connection="Output">
<RecordInfo>
	<Field name="Field1" size="1" source="TextInput:" type="String"/>
	<Field name="Field2" source="TextInput:" type="Byte"/>
</RecordInfo>
</MetaInfo>
`

func getUserIdFromRecordBlob(record goyxdb.RecordBlob) int {
	return int(*((*uint32)(record.Blob())))
}
