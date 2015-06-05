package dims

import (
	"bytes"
	"encoding/json"
	"log"
	"sort"
	"testing"
)

type RawSource struct {
	Value1 string
	Value2 string
	Name   string
}

var data = []RawSource{
	RawSource{"Walk in", "Washington", "Amy"},
	RawSource{"Email", "California", "Tony"},
	RawSource{"Walk in", "California", "Tony"},
	RawSource{"Email", "California", "Tony"},
	RawSource{"Call", "Oregon", "Amy"},
}

type Value1_DimDef struct {
}

func Value1_Dim() *Value1_DimDef {
	return &Value1_DimDef{}
}

func (dim *Value1_DimDef) Legend() interface{} {
	return "Action"
}

func (dim *Value1_DimDef) GetPartitionMap(data interface{}) (d []interface{}, err error) {
	d, err = MapPartition(data, dim)
	sort.Sort(ByAlpha(d))
	return
}

func (dim *Value1_DimDef) Partition(fact interface{}) (key interface{}, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = f.Value1
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

func (dim *Value2_DimDef) Legend() interface{} {
	return "State"
}

func (dim *Value2_DimDef) GetPartitionMap(data interface{}) (d []interface{}, err error) {
	d, err = MapPartition(data, dim)
	sort.Sort(ByAlpha(d))
	return
}

func (dim *Value2_DimDef) Partition(fact interface{}) (key interface{}, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = f.Value2
	default:
		err = ErrInvalidPartition(f)
	}
	return
}

type Name_DimDef struct {
}

func Name_Dim() *Name_DimDef {
	return &Name_DimDef{}
}

func (dim *Name_DimDef) Legend() interface{} {
	return "Name"
}

func (dim *Name_DimDef) GetPartitionMap(data interface{}) (d []interface{}, err error) {
	d, err = MapPartition(data, dim)
	sort.Sort(ByAlpha(d))
	return
}

func (dim *Name_DimDef) Partition(fact interface{}) (key interface{}, err error) {
	switch f := fact.(type) {
	case RawSource:
		key = f.Name
	default:
		err = ErrInvalidPartition(f)
	}
	return
}

/*
func Test_ZeroDataSet(t *testing.T) {
	d1 := Value1_Dim()
	_, err := BuildOneDimReport([]RawSource{}, d1)
	if err != nil {
		t.Errorf("Expected valid output [ %s ]", err)
	}
}
*/
/*
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
*/

func Test_TableReport(t *testing.T) {
	cfg := &ReportConfig{
		Map:     CounterMapper,
		AccFunc: IntSumAccumulator,
		Dims:    Dims(Set(Value1_Dim()), Set(Value2_Dim(), Name_Dim())),
	}

	report, err := TableReport(data, cfg)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(report)
	log.Printf("Two Dim Grid Report:\n[err:%s]\n[\n%#v\n]\n\njson:\n%s\n\n", err, report, buf)
}

/*
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
*/
