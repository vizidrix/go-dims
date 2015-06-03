package dims

import (
	"bytes"
	"encoding/json"
	"log"
	"testing"
)

type RawSource struct {
	Value1 string
	Value2 string
}

var data = []RawSource{
	RawSource{"Walk in", "Washington"},
	RawSource{"Email", "California"},
	RawSource{"Walk in", "California"},
	RawSource{"Email", "California"},
	RawSource{"Call", "Oregon"},
}

type Value1_DimDef struct {
}

func Value1_Dim() *Value1_DimDef {
	return &Value1_DimDef{}
}

func (dim *Value1_DimDef) Display() string {
	return "Action"
}

func (dim *Value1_DimDef) GetPartitions(data interface{}) (u []string, d [][]string, err error) {
	return MapPartitions(data, dim)
}

func (dim *Value1_DimDef) Partition(fact interface{}) (key string, value int64, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = f.Value1
		value = 1
	default:
		err = ErrInvalidPartition(f)
	}
	return
}

type Value2_DimDef struct {
}

func Value2_Dim() *Value2_DimDef {
	return &Value2_DimDef{}
}

func (dim *Value2_DimDef) Display() string {
	return "State"
}

func (dim *Value2_DimDef) GetPartitions(data interface{}) (u []string, d [][]string, err error) {
	return MapPartitions(data, dim)
}

func (dim *Value2_DimDef) Partition(fact interface{}) (key string, value int64, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = f.Value2
		value = 1
	default:
		err = ErrInvalidPartition(f)
	}
	return
}

func Test_ZeroDataSet(t *testing.T) {
	d1 := Value1_Dim()
	_, err := BuildOneDimReport([]RawSource{}, d1)
	if err != nil {
		t.Errorf("Expected valid output [ %s ]", err)
	}
}

func Test_OneDimReport(t *testing.T) {
	d1 := Value1_Dim()
	report, err := BuildOneDimReport(data, d1)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
		return
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(report)
	log.Printf("One Dim Report [err:%s]: [\n%#v\n]\n%s\n\n", err, report, buf)
}

func Test_TwoDimGridReport(t *testing.T) {
	d1 := Value1_Dim()
	d2 := Value2_Dim()

	report, err := BuildTwoDimGridReport(data, d1, d2)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(report)
	log.Printf("Two Dim Grid Report [err:%s]: [\n%#v\n]\n%s\n\n", err, report, buf)
}

func Test_MultiDimension_TwoGridReport(t *testing.T) {
	d1 := Value1_Dim()
	d2 := Value2_Dim()

	report, err := BuildTwoDimGridReport(data, MergeDim(" / ", d1, d2), d2)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(report)
	log.Printf("Two Dim Grid Report [err:%s]: [\n%#v\n]\n%s\n\n", err, report, buf)

}
