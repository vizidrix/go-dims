package dims

import (
	//"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	//"sort"
)

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
	Value interface{}
	Index int
}

type DimPathDef struct {
	Key   DimKey
	More  interface{}
	Depth int
}

func DimPath(keys ...DimKey) DimPathDef {
	var more interface{}
	l := len(keys)
	if l > 1 {
		more = DimPath(keys[1:]...)
	}
	return DimPathDef{keys[0], more, l}
}

func (p DimPathDef) Flatten() (r []DimKey) {
	r = make([]DimKey, p.Depth, p.Depth)

	return
}

type BucketPathDef struct {
	Key   DimPathDef
	More  interface{}
	Depth int
}

func BucketPath(keys ...DimPathDef) BucketPathDef {
	var more interface{}
	l := len(keys)
	if l > 1 {
		more = BucketPath(keys[1:]...)
	}
	return BucketPathDef{keys[0], more, l}
}

func (p BucketPathDef) Flatten() (r [][]DimKey) {
	r = make([][]DimKey, p.Depth, p.Depth)
	k := p.Key
	m := p.More.(DimPathDef)
	log.Printf("k [ %#v ]", k)
	for i := 0; i < p.Depth; i++ {
		r[i] = k.Flatten()
		log.Printf("\n\nFlattened [ %#v ]", r[i])
		log.Printf("\n\nMore [ %#v ]", m)
		//k = k.More.(DimPathDef)
		k = m
		m = k.More.(DimPathDef)
	}
	return
}

type BucketAccumulator map[BucketPathDef]Accumulator

func (t BucketAccumulator) MarshalJSON() ([]byte, error) {
	for k, v := range t {
		log.Printf("\nK[%#v] V[%#v]\n", k, v.Value())
		path := k.Flatten()
		log.Printf("Path [ %#v ]", path)
		/*
			keys := make([]JsonDimKey, 0, 1)
			for ; k.More != nil; k = k.More.(BucketPathDef) {
				keys = append(keys, JsonDimKey{fmt.Sprintf("%s", k.Key.Key.Value), k.Key.Key.Index})
				log.Printf("\n\n* Keys [ %#v ]", keys)
			}
		*/
	}
	return nil, errors.New("err")
}

type TableReportDef struct {
	Legends   [][]interface{}
	Keys      [][][]interface{}
	SubTotals [][]Accumulator
	Data      BucketAccumulator
	Total     Accumulator
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
	r.SubTotals = make([][]Accumulator, s_l, s_l)
	r.Data = make(map[BucketPathDef]Accumulator)
	dimkeys := make([][]DimKey, s_l, s_l)
	dimpaths := make([]DimPathDef, s_l, s_l)
	dimlen := make([]int, s_l, s_l)
	keymaps := make([]map[interface{}]int, s_l, s_l)
	for s = 0; s < s_l; s++ { // For each dim set build def dimension
		dimlen[s] = len(c.Dims[s].Dims)
		d_l = dimlen[s]
		dimkeys[s] = make([]DimKey, d_l, d_l)
		r.Legends[s] = c.Dims[s].Legends()
		if r.Keys[s], err = c.Dims[s].GetPartitionMap(data); err != nil {
			return
		}
		keymaps[s] = make(map[interface{}]int)
		r.SubTotals[s] = make([]Accumulator, d_l, d_l)
		for d = 0; d < d_l; d++ {
			r.SubTotals[s][d] = c.AccFunc()
			keys := r.Keys[s][d]
			k_l = len(keys)
			for k = 0; k < k_l; k++ {
				keymaps[s][keys[k]] = k
			}
		}
	}
	err = MapSlice(data, func(fact interface{}) error {
		for s = 0; s < s_l; s++ {
			keys, err = c.Dims[s].Partition(fact)
			for d = 0; d < dimlen[s]; d++ {
				dimkeys[s][d].Value = keys[d]
				dimkeys[s][d].Index = keymaps[s][keys[d]] // map to key index of value in dim
			}
			dimpaths[s] = DimPath(dimkeys[s]...)
		}
		b := BucketPath(dimpaths...)
		if a, ok = r.Data[b]; !ok {
			r.Data[b] = c.AccFunc()
			a = r.Data[b]
		}
		a.Acc(c.Map(fact))
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
	defer func() {
		log.Printf("DimSet GetPartitionMap keys[\n%#v\n]", keys)
	}()
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
		//keys = append(keys, s.Dims[i].Partition(fact)...)
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

//u = make([]string, dim_l, dim_l)
//c := make([]int, dim_l, dim_l)
//perms := 1

//sort.Sort(ByAlpha(d[i]))
//c[i] = len(d[i])
//perms *= c[i]

//u = make([]string, perms, perms)
/*
	p_div := perms
	for i := 0; i < dim_l; i++ {
		d_l := len(accs[i])
		if d_l > 0 {
			p_div /= d_l
			if p_div == 0 {
				p_div = 1
			}
		}
		for p := 0; p < perms; p++ {
			p_i := (p / p_div) % d_l
			if i > 0 {
				u[p] += " "
			}
			u[p] += d[i][p_i]
		}
	}
*/
//	return
//}

/*
type TableReportDef struct {
	Legends   [2][]interface{}
	Keys      [2][][]interface{}
	SubTotals [2]map[int]interface{}
	//Dim1UnionKeys []string
	//Dim1Keys      [][]string
	//Dim2UnionKeys []string
	//Dim2Keys      [][]string
	//Data map[string]map[string]interface{} // Dim1 / Dim2 = Value
	//Data map[int]map[int]interface{} // Dim1 / Dim2 = Value
	Data map[DimPathDef]map[DimPathDef]interface{}

	//Dim1Totals    []int64
	//Dim2Totals    []int64
	Total interface{}
}
*/
//Dim1UnionKeys []string
//Dim1Keys      [][]string
//Dim2UnionKeys []string
//Dim2Keys      [][]string
//Data map[string]map[string]interface{} // Dim1 / Dim2 = Value
//Data map[int]map[int]interface{} // Dim1 / Dim2 = Value
//Data map[DimPathDef]map[DimPathDef]interface{}
//Dim1Totals    []int64
//Dim2Totals    []int64
//Dim1 DimSet
//Dim2 DimSet
/*
		Legends: [2][]string{
			cfg.Dim1.Legends(),
			cfg.Dim2.Legends(),
		},
	}
	if r.Keys[0], err = cfg.Dim1.GetPartitionMap(data); err != nil {
		return
	}
	if r.Keys[1], err = cfg.Dim2.GetPartitionMap(data); err != nil {
		return
	}
	r.SubTotals[0] = make(map[int]interface{})
	r.SubTotals[1] = make(map[int]interface{})
	l0 := len(r.Legends[0])
	l1 := len(r.Legends[1])
	km0 := make([]map[interface{}]int, l0, l0)
	km1 := make([]map[interface{}]int, l1, l1)
	var i, j, k_l int
	for i = 0; i < l0; i++ {
		km0[i] = make(map[interface{}]int)
		k := r.Keys[0][i] // Keys for dimension
		k_l = len(k)
		for j = 0; j < k_l; j++ {
			km0[i][k[j]]
		}
	}

	err = MapSlice(data, func(fact interface{}) error {
		k1, err = dim1.Partition(fact)
		if err != nil {
			return err
		}
		k2, err = dim2.Partition(fact)
		if err != nil {
			return err
		}
		p1 := DimPath(k1...)
		data_map[k1][k2] += f(fact)
		return nil
	})
	//r.SubTotals[0] = make([]int64, k0, k0)
	//r.SubTotals[1] = make([]int64, k1, k1)
}
*/

/*
type OneDimReport struct {
	Legends   []string
	UnionKeys []string
	Keys      []string
	Data      []int64
	Total     int64
}

func BuildOneDimReport(data interface{}, dim DimPartitioner) (r *OneDimReport, err error) {
	var pss [2][]string
	if pss, err = dim.GetPartitions(data); err != nil {
		return
	}
	ps := pss[0]
	l := len(ps)
	r = &OneDimReport{
		Legends:   dim.Legend(),
		UnionKeys: ps,
		Keys:      ps,
		Data:      make([]int64, l, l),
		Total:     0,
	}
	data_map := make(map[string]int64)
	for i := 0; i < l; i++ {
		data_map[ps[i]] = 0
	}
	err = MapSlice(data, func(fact interface{}) error {
		k, v, err := dim.Partition(fact)
		if err != nil {
			return err
		}
		log.Printf("Key: [ %s ]", k)
		data_map[k] += v
		r.Total += v
		return nil
	})
	log.Printf("DataMap:\n%#v", data_map)
	for i, p := range ps {
		r.Data[i] = data_map[p]
	}
	return
}
*/

/*
func BuildTwoDimGridReport(data interface{}, m Mapper, acc Accumulator, dim1, dim2 DimPartitioner) (r *TwoDimReport, err error) {
	r = &TwoDimReport{
		Legends: [2][]string{
			dim1.Legend(),
			dim2.Legend(),
		},
	}
	if r.Keys[0], err = dim1.GetPartitions(data); err != nil {
		return
	}
	if r.Keys[1], err = dim2.GetPartitions(data); err != nil {
		return
	}
	l0 := len(r.Legends[0])
	l1 := len(r.Legends[1])
	k0 := 0
	k1 := 0
	for i := 0; i < l0; i++ {
		k0 *= len(r.Keys[0][i])
	}
	for i := 0; i < l1; i++ {
		k1 *= len(r.Keys[1][i])
	}
	r.SubTotals[0] = make([]int64, k0, k0)
	r.SubTotals[1] = make([]int64, k1, k1)

	//r.Data = make([][]int64, l0, l0)
	//for i := 0; i < l1; i++ {
	//	r.Data[i] = make([]int64, l1, l1)
	//}

	km0 := make([]map[string]int, l0, l0) // For each sub dim
	data_map := make(map[string]map[string]int64)
	for _, k1s := range r.Keys[0] {
		data_map[du1] = make(map[string]int64)
		for _, du2 := range r.Keys[1] {
			data_map[du1][du2] = 0
		}
	}
	var k1 []string
	var k2 []string
	err = MapSlice(data, func(fact interface{}) error {
		k1, err = dim1.Partition(fact)
		if err != nil {
			return err
		}
		k2, err = dim2.Partition(fact)
		if err != nil {
			return err
		}
		data_map[k1][k2] += f(fact)
		return nil
	})
	for i, p1 := range r.Dim1UnionKeys {
		for j, p2 := range r.Dim2UnionKeys {
			v := data_map[p1][p2]
			r.Data[i][j] = v
			r.Totals[0][i] += v
			r.Totals[1][j] += v
			r.Total += v
		}
	}
	return
}
*/

/*
fact, ok := in.(Fact)
		if !ok {
			return errors.New(fmt.Sprintf("Invalid fact [ %#v ]", in))
		}
fact_v := reflect.ValueOf(fact)
		if fact_v.Kind() == reflect.Ptr {
			fact_v = fact_v.Elem()
		}
		fact_type := reflect.TypeOf(i)
*/
/*
func MapPartitions(data interface{}, dim DimPartitioner) (keys [][]interface{}, err error) {

}
func MapPartitions(data interface{}, dims ...DimPartitioner) (u []string, d [][]string, err error) {
	dim_l := len(dims)
	accs := make([]map[string]struct{}, dim_l, dim_l)
	for i := 0; i < dim_l; i++ {
		accs[i] = make(map[string]struct{})
	}
	if err = MapSlice(data, func(fact interface{}) error {
		for i, dim := range dims {
			k, _, err := dim.Partition(fact)
			if err != nil {
				return err
			}
			accs[i][k] = struct{}{}
		}
		return nil
	}); err != nil {
		return
	}
	d = make([][]string, dim_l, dim_l)
	u = make([]string, dim_l, dim_l)
	c := make([]int, dim_l, dim_l)
	perms := 1
	for i := 0; i < dim_l; i++ { // For each dimension provided
		d_l := len(accs[i]) // Length of unique partitions in each dimension
		d[i] = make([]string, d_l, d_l)
		j := 0
		for k, _ := range accs[i] {
			d[i][j] = k
			j++
		}
		sort.Sort(ByAlpha(d[i]))
		c[i] = len(d[i])
		perms *= c[i]
	}
	u = make([]string, perms, perms)
	p_div := perms
	for i := 0; i < dim_l; i++ {
		d_l := len(accs[i])
		if d_l > 0 {
			p_div /= d_l
			if p_div == 0 {
				p_div = 1
			}
		}
		for p := 0; p < perms; p++ {
			p_i := (p / p_div) % d_l
			if i > 0 {
				u[p] += " "
			}
			u[p] += d[i][p_i]
		}
	}
	return
}
*/

/*
type MergeDimDef struct {
	Dims []DimPartitioner
}

func MergeDim(dims ...DimPartitioner) DimPartitioner {
	return &MergeDimDef{
		Dims: dims,
	}
}

func (dim *MergeDimDef) Legends() (r []interface{}) {
	l := len(dim.Dims)
	r = make([]interface{}, 0, l)
	//buf := &bytes.Buffer{}
	for i := 0; i < l; i++ {
		r = append(r, dim.Dims[i].Legends()...)
	}
	return r
	//if i > 0 {
	//	buf.WriteString(dim.separator)
	//}
	//buf.WriteString(dim.Dims[i].Display())
	//}
	//return buf.String()
}

func (dim *MergeDimDef) GetPartitionMap(data interface{}) (keys [][]interface{}, err error) {
	dim_l := len(dim.Dims)
	acc := make([][]interface{}, dim_l, dim_l)
	keys, err = MapPartitions(data, dim.Dims...)
	log.Printf("MapPartitions keys[\n%#v\n]", keys)
	return
}

func (dim *MergeDimDef) Partition(fact interface{}) (keys []interface{}, err error) {
	l := len(dim.Dims)
	keys = make([]interface{}, 0, l)
	for i := 0; i < l; i++ {
		keys = append(keys, dim.Dims[i].Partition(fact)...)
	}
	return
}
*/
/*
	buf := &bytes.Buffer{}
	var k string
	for i := 0; i < l; i++ {
		if i > 0 {
			buf.WriteString(" ")
		}
		k, value, err = dim.Dims[i].Partition(fact)
		buf.WriteString(k)
	}
	key = buf.String()
	return
}
*/
