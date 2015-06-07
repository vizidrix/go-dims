package dims

import (
	//"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	//"sort"
)

var ErrEndOfPath = errors.New("End of path")

func ErrInvalidPartition(p interface{}) error {
	return errors.New(fmt.Sprintf("Invalid partition [ %#v ]", p))
}

type ByAlpha []interface{}

func (s ByAlpha) Len() int {
	return len(s)
}

func (s ByAlpha) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByAlpha) Less(i, j int) bool {
	return s[i].(string) < s[j].(string)
}

type DimPartitioner interface {
	Legend() interface{}
	GetPartitionMap(data interface{}) (keys []interface{}, err error)
	Partition(data interface{}) (key interface{}, err error)
}

type Mapper func(v interface{}) interface{}

type Accumulator interface {
	Acc(v interface{})
	Value() interface{}
}

type AccumulatorFunc func() Accumulator

func CounterMapper(v interface{}) interface{} {
	return int(1)
}

type IntSumAccumulatorDef struct {
	value int
}

func (acc *IntSumAccumulatorDef) Acc(v interface{}) {
	if v_int, ok := v.(int); ok {
		acc.value += v_int
		return
	}
	panic(fmt.Sprintf("Accumulator int sum expected int value but was [ %#v ]", v, reflect.TypeOf(v).Name()))
}

func (acc *IntSumAccumulatorDef) Value() interface{} {
	return acc.value
}

func IntSumAccumulator() Accumulator {
	return &IntSumAccumulatorDef{0}
}

type DimKey struct {
	Value interface{} `json:"v"`
	Index int         `json:"i"`
}

type DimPathDef struct {
	Current DimKey
	Next    interface{}
	Depth   int
}

func DimPath(keys ...DimKey) DimPathDef {
	var next interface{}
	l := len(keys)
	if l > 1 {
		next = DimPath(keys[1:]...)
	}
	return DimPathDef{keys[0], next, l}
}

func (p DimPathDef) GetNext() (r DimPathDef, err error) {
	var ok bool
	if p.Depth > 0 {
		if r, ok = (p.Next).(DimPathDef); ok {
			return
		}
	}
	err = ErrEndOfPath
	return
}

func (p DimPathDef) GetKeys() (r []DimKey, err error) {
	r = make([]DimKey, 0, p.Depth)
	for {
		r = append(r, p.Current)
		if p, err = p.GetNext(); err != nil {
			if err == ErrEndOfPath {
				err = nil
			}
			return
		}
	}
	return
}

type BucketPathDef struct {
	Current DimPathDef
	Next    interface{}
	Depth   int
}

func BucketPath(keys ...DimPathDef) BucketPathDef {
	var next interface{}
	l := len(keys)
	if l > 1 {
		next = BucketPath(keys[1:]...)
	}
	return BucketPathDef{keys[0], next, l}
}

func (b BucketPathDef) GetNext() (r BucketPathDef, err error) {
	var ok bool
	if b.Depth >= 0 {
		if r, ok = (b.Next).(BucketPathDef); !ok {
			err = ErrEndOfPath
			return
		}
	} else {
		err = ErrEndOfPath
	}
	return
}

func (b BucketPathDef) GetDimPaths() (r []DimPathDef, err error) {
	r = make([]DimPathDef, 0, b.Depth)
	for {
		r = append(r, b.Current)
		if b, err = b.GetNext(); err != nil {
			if err == ErrEndOfPath {
				err = nil
			}
			return
		}
	}
	return
}

func (b BucketPathDef) GetKeys() (r [][]DimKey, err error) {
	var ps []DimPathDef
	if ps, err = b.GetDimPaths(); err != nil {
		return
	}
	l := len(ps)
	r = make([][]DimKey, l, l)
	for i, p := range ps {
		if r[i], err = p.GetKeys(); err != nil {
			return
		}
	}
	return
}

type DimAccumulator map[DimPathDef]Accumulator
type BucketAccumulator map[BucketPathDef]Accumulator

func (t DimAccumulator) GetData() (r map[string]interface{}, err error) {
	var ds []DimKey
	r = make(map[string]interface{})
	var s map[string]interface{}
	var q interface{}
	var ok bool
	//log.Printf("\n* Subs [ %#v ]", t)
	var k string
	for d, acc := range t {
		s = r
		ds, err = d.GetKeys()
		//log.Printf("\n* Keys [ %#v ]", ds)
		d_l := len(ds)
		for d := 0; d < d_l-1; d++ {
			//dk := ds[d]
			//log.Printf("\n* Key [ %#v ]", dk)
			k = strconv.FormatInt(int64(ds[d].Index), 10)
			if q, ok = s[k]; !ok {
				q = make(map[string]interface{})
				s[k] = q
			}
			s = q.(map[string]interface{})
		}
		k = strconv.FormatInt(int64(ds[d_l-1].Index), 10)
		s[k] = acc.Value()
	}
	return
}

func (t BucketAccumulator) GetData() (r map[string]interface{}, err error) {
	var ds [][]DimKey
	r = make(map[string]interface{}) // Container
	var ok bool
	var k string
	var i int
	var s map[string]interface{}
	s = r
	var q interface{}
	for b, acc := range t {
		ds, err = b.GetKeys()
		d_l := len(ds)
		for d := 0; d < d_l; d++ {
			k_l := len(ds[d])
			for i = 0; i < k_l; i++ {
				k = strconv.FormatInt(int64(ds[d][i].Index), 10)
				if d == d_l-1 && i == k_l-1 { // Check for last entry of the last dim
					s[k] = acc.Value()
				} else {
					if q, ok = s[k]; !ok { // Index not found in map
						q = make(map[string]interface{})
						s[k] = q
					}
					s = q.(map[string]interface{})
				}
			}
		}
	}
	return
}

func (t BucketAccumulator) MarshalJSON() (b []byte, err error) {
	for k, _ := range t {
		for k, err = k.GetNext(); err == nil; k, err = k.GetNext() {
			log.Printf("\n\n%#v", k)
		}
	}
	return nil, errors.New("err")
}

type TableReportDef struct {
	Legends   [][]interface{}
	Keys      [][][]interface{}
	SubTotals []DimAccumulator
	Data      BucketAccumulator
	Total     Accumulator
}

type TableReportViewModel struct {
	Legends   [][]string
	Keys      [][][]string
	SubTotals []interface{}
	Data      interface{}
	Total     interface{}
}

func (t *TableReportDef) ToViewModel() (vm *TableReportViewModel, err error) {
	vm = &TableReportViewModel{}
	// Legends
	l := len(t.Legends)
	vm.Legends = make([][]string, l, l)
	for i := 0; i < l; i++ {
		l_i := len(t.Legends[i])
		vm.Legends[i] = make([]string, l_i, l_i)
		for j := 0; j < l_i; j++ {
			vm.Legends[i][j] = fmt.Sprintf("%s", t.Legends[i][j])
		}
	}
	// Keys
	l_k := len(t.Keys)
	vm.Keys = make([][][]string, l_k, l_k)
	for i := 0; i < l; i++ {
		l_i := len(t.Keys[i])
		vm.Keys[i] = make([][]string, l_i, l_i)
		for j := 0; j < l_i; j++ {
			l_j := len(t.Keys[i][j])
			vm.Keys[i][j] = make([]string, l_j, l_j)
			for k := 0; k < l_j; k++ {
				vm.Keys[i][j][k] = fmt.Sprintf("%s", t.Keys[i][j][k])
			}
		}
	}
	// SubTotals
	vm.SubTotals = make([]interface{}, l, l)
	for i := 0; i < l; i++ {
		if vm.SubTotals[i], err = t.SubTotals[i].GetData(); err != nil {
			return
		}
	}

	// Data
	if vm.Data, err = t.Data.GetData(); err != nil {
		return
	}

	// Total
	vm.Total = t.Total.Value()
	return
}

type ReportConfig struct {
	Map     Mapper
	AccFunc AccumulatorFunc
	Dims    []DimSet
}

func TableReport(data interface{}, c *ReportConfig) (r *TableReportDef, err error) {
	var s, s_l, d, d_l, k, k_l int
	var keys []interface{}
	var a Accumulator
	var ok bool
	r = &TableReportDef{
		Total: c.AccFunc(),
	}
	s_l = len(c.Dims)
	r.Legends = make([][]interface{}, s_l, s_l)
	r.Keys = make([][][]interface{}, s_l, s_l)
	r.Data = make(map[BucketPathDef]Accumulator)
	dimkeys := make([][]DimKey, s_l, s_l)
	dimpaths := make([]DimPathDef, s_l, s_l)
	dimlen := make([]int, s_l, s_l)
	dimkeylen := make([][]int, s_l, s_l)
	keymaps := make([]map[interface{}]int, s_l, s_l)
	r.SubTotals = make([]DimAccumulator, s_l, s_l)
	for s = 0; s < s_l; s++ { // For each dim set build def dimension
		dimlen[s] = len(c.Dims[s].Dims)
		d_l = dimlen[s]
		dimkeys[s] = make([]DimKey, d_l, d_l)
		dimkeylen[s] = make([]int, d_l, d_l)
		r.Legends[s] = c.Dims[s].Legends()
		if r.Keys[s], err = c.Dims[s].GetPartitionMap(data); err != nil {
			return
		}
		keymaps[s] = make(map[interface{}]int)
		r.SubTotals[s] = make(map[DimPathDef]Accumulator)
		for d = 0; d < d_l; d++ {
			keys := r.Keys[s][d]
			k_l = len(keys)
			dimkeylen[s][d] = k_l
			for k = 0; k < k_l; k++ {
				keymaps[s][keys[k]] = k
			}
		}
	}
	st := make([]DimAccumulator, s_l, s_l)
	err = MapSlice(data, func(fact interface{}) error {
		v := c.Map(fact)
		for s = 0; s < s_l; s++ {
			st[s] = make(map[DimPathDef]Accumulator)
			keys, err = c.Dims[s].Partition(fact)
			for d = 0; d < dimlen[s]; d++ {
				dimkeys[s][d].Value = keys[d]
				dimkeys[s][d].Index = keymaps[s][keys[d]] // map to key index of value in dim
			}
			dimpaths[s] = DimPath(dimkeys[s]...)
			if _, ok = r.SubTotals[s][dimpaths[s]]; !ok {
				r.SubTotals[s][dimpaths[s]] = c.AccFunc()
			}
			r.SubTotals[s][dimpaths[s]].Acc(v)
		}
		b := BucketPath(dimpaths...)
		if a, ok = r.Data[b]; !ok {
			r.Data[b] = c.AccFunc()
			a = r.Data[b]
		}
		a.Acc(v)
		r.Total.Acc(v)
		return nil
	})
	return
}

type DimSet struct {
	Dims []DimPartitioner
}

func Set(dims ...DimPartitioner) DimSet {
	return DimSet{
		Dims: dims,
	}
}

func Dims(sets ...DimSet) (r []DimSet) {
	return sets
}

func (s *DimSet) Legends() (r []interface{}) {
	l := len(s.Dims)
	r = make([]interface{}, l, l)
	for i := 0; i < l; i++ {
		r[i] = s.Dims[i].Legend()
	}
	return r
}

func (s *DimSet) GetPartitionMap(data interface{}) (keys [][]interface{}, err error) {
	l := len(s.Dims)
	keys = make([][]interface{}, l, l)
	for i := 0; i < l; i++ {
		if keys[i], err = s.Dims[i].GetPartitionMap(data); err != nil {
			return
		}
	}
	return
}

func (s *DimSet) Partition(fact interface{}) (keys []interface{}, err error) {
	l := len(s.Dims)
	keys = make([]interface{}, l, l)
	for i := 0; i < l; i++ {
		if keys[i], err = s.Dims[i].Partition(fact); err != nil {
			return
		}
	}
	return
}

func MapSlice(t interface{}, f func(interface{}) error) (err error) {
	t_type := reflect.TypeOf(t)
	if t_type.Kind() == reflect.Ptr {
		t_type = t_type.Elem()
	}
	switch t_type.Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(t)
		if s.Kind() == reflect.Ptr {
			s = s.Elem()
		}
		l := s.Len()
		for i := 0; i < l; i++ {
			e := s.Index(i)
			if e.Kind() == reflect.Ptr {
				e = e.Elem()
			}
			if err = f(e.Interface()); err != nil {
				return err
			}
		}
	default:
		err = errors.New("Value was not a slice")
	}
	return
}

func MapPartition(data interface{}, dim DimPartitioner) (d []interface{}, err error) {
	var ds [][]interface{}
	ds, err = MapPartitions(data, dim)
	d = ds[0]
	return
}
func MapPartitions(data interface{}, dims ...DimPartitioner) (d [][]interface{}, err error) {
	dim_l := len(dims)
	accs := make([]map[interface{}]struct{}, dim_l, dim_l)
	for i := 0; i < dim_l; i++ {
		accs[i] = make(map[interface{}]struct{})
	}
	if err = MapSlice(data, func(fact interface{}) error {
		for i, dim := range dims {
			k, err := dim.Partition(fact)
			if err != nil {
				return err
			}
			accs[i][k] = struct{}{}
		}
		return nil
	}); err != nil {
		return
	}
	d = make([][]interface{}, dim_l, dim_l)
	for i := 0; i < dim_l; i++ { // For each dimension provided
		d_l := len(accs[i]) // Length of unique partitions in each dimension
		d[i] = make([]interface{}, d_l, d_l)
		j := 0
		for k, _ := range accs[i] {
			d[i][j] = k
			j++
		}
	}
	return
}
