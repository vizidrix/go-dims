package dims

import (
	"bytes"
	"encoding/json"
	. "github.com/stretchr/testify/assert"
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

func Test_DimPathDef(t *testing.T) {
	p := DimPath(
		DimKey{"key1", 0},
		DimKey{"key2", 1},
		DimKey{"key3", 2},
	)
	Equal(t, 3, p.Depth)
	Equal(t, "key1", p.Current.Value, "current")
	n, err := p.GetNext()
	NoError(t, err, "get next")
	Equal(t, 1, n.Current.Index, "get next")
	Equal(t, "key2", n.Current.Value, "get next")
	ks, err := p.GetKeys()
	NoError(t, err, "flatten")
	Equal(t, "key1", ks[0].Value, "flattened - 1")
	Equal(t, "key2", ks[1].Value, "flattened - 2")
	n2, err := n.GetNext()
	NoError(t, err, "get next 2")
	NotNil(t, n2)
	n3, err := n2.GetNext()
	Error(t, err, "end of path")
	Equal(t, 0, n3.Depth)
}

func Test_BucketPathDef(t *testing.T) {
	p1 := DimPath(DimKey{"key1", 0}, DimKey{"key2", 1}, DimKey{"key3", 2})
	p2 := DimPath(DimKey{"key4", 3}, DimKey{"key5", 4}, DimKey{"key6", 5})
	p3 := DimPath(DimKey{"key7", 6}, DimKey{"key8", 7}, DimKey{"key9", 8})

	b := BucketPath(p1, p2, p3)

	Equal(t, 3, b.Depth)
	Equal(t, "key1", b.Current.Current.Value, "current")
	n, err := b.GetNext()
	NoError(t, err, "get next")
	Equal(t, "key4", n.Current.Current.Value, "get next")
	dims, err := b.GetDimPaths()
	NoError(t, err, "flatten")
	Equal(t, 3, len(dims))
	Equal(t, "key1", dims[0].Current.Value)
	Equal(t, "key4", dims[1].Current.Value)
	Equal(t, "key7", dims[2].Current.Value)
	n2, err := n.GetNext()
	NoError(t, err, "get next 2")
	NotNil(t, n2)

	ks, err := b.GetKeys()
	NoError(t, err)
	Equal(t, 3, len(ks), "get keys")
	Equal(t, 3, len(ks[0]), "get keys 0")
}

func Test_BucketPathDef_Json(t *testing.T) {
	p1 := DimPath(DimKey{"key1", 0}, DimKey{"key2", 1}, DimKey{"key3", 2})
	p2 := DimPath(DimKey{"key4", 3}, DimKey{"key5", 4}, DimKey{"key6", 5})

	b := BucketPath(p1, p2)

	ks, err := b.GetKeys()
	NoError(t, err)

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(ks)
	NoError(t, err)
}

func Test_TableReport(t *testing.T) {
	cfg := &ReportConfig{
		Map:     CounterMapper,
		AccFunc: IntSumAccumulator,
		Dims:    Dims(Set(Value1_Dim(), Name_Dim()), Set(Value2_Dim(), Name_Dim())),
		//Dims:    Dims(Set(Value1_Dim()), Set(Value2_Dim(), Name_Dim())),
		//Dims: Dims(Set(Value1_Dim(), Value2_Dim()), Set(Name_Dim())),
	}
	report, err := TableReport(data, cfg)
	if err != nil {
		t.Errorf("Error building report [ %s ]", err)
	}
	vm, err := report.ToViewModel()
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)

	g, err := vm.ToGrid()
	//err = enc.Encode(vm)
	err = enc.Encode(g)

	//log.Printf("Two Dim Grid Report:\n[err:%s]\n\nkeys:\n%s\n\njson:\n%s\n\n", err, report.Keys, buf)

}
