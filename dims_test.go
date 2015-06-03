package dims

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
)

var ()

func ErrInvalidPartition(fact interface{}) error {
	return errors.New(fmt.Sprintf("Invalid fact found [ %#v ]", fact))
}

type RawSource struct {
	Value1 int
	Value2 int
}

var data = []RawSource{
	RawSource{1, 1},
	RawSource{2, 3},
	RawSource{1, 3},
	RawSource{1, 1},
	RawSource{3, 5},
}

type Value1_DimDef struct {
}

func Value1_Dim() *Value1_DimDef {
	return &Value1_DimDef{}
}

func (dim *Value1_DimDef) Display() string {
	return "Value 1"
}

func (dim *Value1_DimDef) GetPartitions(data interface{}) (u []string, d [][]string, err error) {
	return MapPartitions(data, dim)
}

func (dim *Value1_DimDef) Partition(fact interface{}) (key string, value int64, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = fmt.Sprintf("%d", f.Value1)
		value = 1
	default:
		err = ErrInvalidPartition(f)
	}
	return
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

	report, err := BuildTwoDimGridReport(data, d1, d1)
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

	report, err := BuildTwoDimGridReport(data, MergeDim(d1, d1), d1)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(report)
	log.Printf("Two Dim Grid Report [err:%s]: [\n%#v\n]\n%s\n\n", err, report, buf)

}
