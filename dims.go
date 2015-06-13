package dims

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
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

type DimEntry struct {
	Path DimPathDef
	Accumulator
}

type DimSorter []*DimEntry

func (d DimSorter) Len() int {
	return len(d)
}

func (d DimSorter) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d DimSorter) Less(i, j int) bool {
	return fmt.Sprintf("%#v", d[i]) < fmt.Sprintf("%#v", d[j])
}

func (t DimAccumulator) GetSlice() (r []*DimEntry) {
	l := len(t)
	r = make([]*DimEntry, 0, l)
	for d, acc := range t {
		r = append(r, &DimEntry{d, acc})
	}
	sort.Sort(DimSorter(r))
	return
}

func (t DimAccumulator) GetData() (r map[string]interface{}, err error) {
	var ds []DimKey
	r = make(map[string]interface{})
	var s map[string]interface{}
	var q interface{}
	var ok bool
	var k string
	for d, acc := range t {
		s = r
		ds, err = d.GetKeys()
		d_l := len(ds)
		for d := 0; d < d_l-1; d++ {
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

type BucketAccumulator map[BucketPathDef]Accumulator

type BucketEntry struct {
	Path BucketPathDef
	Accumulator
}

type BucketSorter []*BucketEntry

func (b BucketSorter) Len() int {
	return len(b)
}

func (b BucketSorter) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b BucketSorter) Less(i, j int) bool {
	return fmt.Sprintf("%#v", b[i]) < fmt.Sprintf("%#v", b[j])
}

func (t BucketAccumulator) GetSlice() (r []*BucketEntry) {
	l := len(t)
	r = make([]*BucketEntry, 0, l)
	for b, acc := range t {
		r = append(r, &BucketEntry{b, acc})
	}
	sort.Sort(BucketSorter(r))
	return
}

func (t BucketAccumulator) GetData() (r map[string]interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("Error in GetData: %v", r)
			}
		}
	}()
	var ds [][]DimKey
	r = make(map[string]interface{}) // Container
	var ok bool
	var k string
	var i int
	var s map[string]interface{}
	s = r
	var q interface{}
	bes := t.GetSlice()
	for _, be := range bes {
		ds, err = be.Path.GetKeys()
		d_l := len(ds) // Number of keys in the bucket
		for d := 0; d < d_l; d++ {
			k_l := len(ds[d])
			for i = 0; i < k_l; i++ {
				k = strconv.FormatInt(int64(ds[d][i].Index), 10)
				if d == d_l-1 && i == k_l-1 { // Check for last entry of the last dim
					s[k] = be.Accumulator.Value()
				} else {
					if q, ok = s[k]; !ok { // Index not found in map
						q = make(map[string]interface{})
						s[k] = q
					}
					if s, ok = q.(map[string]interface{}); !ok {
						err = fmt.Errorf("Error parsing data: [ %#v ]", s)
					}
				}
			}
		}
	}
	return
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

type TableReportGridViewModel struct {
	Table    [][]string
	Header   [][]string
	SubTotal [][]bool
}

/*
[       |       |  CK1a |  CK1b |       |       |  CL1  |       |  TOT  ]
[       |       |  CK2a |  CK2b |       |       |       |  CL2  |       ]
[  RK1a |  RK2a |   N   |   N   |   N   |   N   |  RS1  |  RS2  |  ###  ]
[  RK1b |  RK2b |   N   |   N   |   N   |   N   |   #   |   #   |  ###  ]
[       |       |   N   |   N   |   N   |   N   |   #   |   #   |  ###  ]
[       |       |   N   |   N   |   N   |   N   |   #   |   #   |  ###  ]
[  RL1  |       |  CS1  |   #   |   #   |   #   |   X   |   X   |  #X#  ]
[       |  RL2  |  CS2  |   #   |   #   |   #   |   X   |   X   |  #X#  ]
[  TOT  |       |  ###  |  ###  |  ###  |  ###  |  #X#  |  #X#  |  XXX  ]
*/
func (t *TableReportViewModel) ToGrid() (g *TableReportGridViewModel, err error) {
	g = &TableReportGridViewModel{}
	i, j := 0, 0
	r, c := 0, 1
	r_keys, c_keys := t.Keys[r], t.Keys[c]
	r_key_l, c_key_l := len(r_keys), len(c_keys)
	r_leg, c_leg := t.Legends[r], t.Legends[c]
	r_leg_l, c_leg_l := len(r_leg), len(c_leg)
	depth := r_leg_l + c_leg_l
	row_i, col_i := 0, 0
	row_perms, col_perms, t_perm_l := 1, 1, 0
	for i = 0; i < c_key_l; i++ {
		col_perms *= len(c_keys[i])
	}
	for i = 0; i < r_key_l; i++ {
		row_perms *= len(r_keys[i])
	}
	// Setup table output
	rows := c_leg_l + row_perms + r_leg_l + 1
	cols := r_leg_l + col_perms + c_leg_l + 1
	g.Table = make([][]string, rows, rows)
	g.Header = make([][]string, rows, rows)
	g.SubTotal = make([][]bool, rows, rows)
	for i = 0; i < rows; i++ {
		g.Table[i] = make([]string, cols, cols)
		g.Header[i] = make([]string, cols, cols)
		g.SubTotal[i] = make([]bool, cols, cols)
	}

	//log.Printf("Subtotals: %#v", t.SubTotals)
	var r_sub map[string]interface{}
	var c_sub map[string]interface{}
	var ok bool
	var sub interface{}
	if r_sub, ok = t.SubTotals[r].(map[string]interface{}); !ok {
		err = fmt.Errorf("Invaild sub totals, %#v", t.SubTotals[r])
		return
	}
	if c_sub, ok = t.SubTotals[c].(map[string]interface{}); !ok {
		err = fmt.Errorf("Invalid sub totals, %#v", t.SubTotals[c])
		return
	}
	// Map row labels
	t_perm_l = row_perms
	for i, k := range r_keys {
		k_l := len(k)
		t_perm_l = t_perm_l / k_l
		for j = 0; j < row_perms; j++ {
			g.Table[j+c_leg_l][i] = k[(j/t_perm_l)%k_l]
			g.Header[j+c_leg_l][i] = "row"
			if sub, ok = r_sub[fmt.Sprintf("%d", j)]; !ok {
				g.Table[j+c_leg_l][col_perms+r_leg_l+i] = "-"
				g.Header[j+c_leg_l][col_perms+r_leg_l+i] = "e-sub"
				continue
			}
			g.Table[j+c_leg_l][col_perms+r_leg_l+i] = fmt.Sprintf("%d", sub)
			g.Header[j+c_leg_l][col_perms+r_leg_l+i] = "sub"
		}
	}
	// Map col labels
	t_perm_l = col_perms
	for i, k := range c_keys {
		k_l := len(k)
		t_perm_l = t_perm_l / k_l
		for j = 0; j < col_perms; j++ {
			g.Table[i][j+r_leg_l] = k[(j/t_perm_l)%k_l]
			g.Header[i][j+r_leg_l] = "col"

			if sub, ok = c_sub[fmt.Sprintf("%d", j)]; !ok {
				g.Table[row_perms+c_leg_l+i][j+r_leg_l] = "-"
				g.Header[row_perms+c_leg_l+i][j+r_leg_l] = "e-sub"
				continue
			}
			g.Table[row_perms+c_leg_l+i][j+r_leg_l] = fmt.Sprintf("%d", sub)
			g.Header[row_perms+c_leg_l+i][j+r_leg_l] = "sub"
		}
	}

	// Fill default values
	for i = 0; i < row_perms; i++ {
		for j = 0; j < col_perms; j++ {
			g.Table[i+c_leg_l][j+r_leg_l] = "-"
			g.Header[i+c_leg_l][j+r_leg_l] = "e-dat"
		}
	}

	// Drop Subtotal Labels
	for i = 0; i < c_leg_l; i++ {
		j = r_leg_l + col_perms + (r_key_l - i - 1)
		g.Table[i][j] = t.Legends[r][i]
		g.Header[i][j] = "r-leg"
	}

	for i = 0; i < r_leg_l; i++ {
		j = c_leg_l + row_perms + (c_key_l - i - 1)
		g.Table[j][i] = t.Legends[c][i]
		g.Header[j][i] = "c-leg"
	}

	// Drop Total
	g.Table[c_leg_l+row_perms][r_leg_l+col_perms] = fmt.Sprintf("%d", t.Total)
	g.Header[c_leg_l+row_perms][r_leg_l+col_perms] = "tot"

	// Walk the data tree and populate the grid
	var walker func(interface{}, int, []string, func([]string, interface{}) error) error
	walker = func(data interface{}, d int, s []string, f func([]string, interface{}) error) (err error) {
		if m, ok := data.(map[string]interface{}); ok {
			for k, v := range m {
				n := make([]string, 0, d)
				n = append(n, s...)
				n = append(n, k)
				err = walker(v, d, n, f)
			}
		} else { // Must be terminal node
			l := len(s)
			if l-d < 0 {
				l = 0
			} else {
				l = l - d
			}
			if l > 0 {
				err = f(s[l:], data)
			} else {
				f(s, data)
			}
		}
		return
	}

	table_walker := func(k []string, v interface{}) (err error) {
		keys := make([]int64, depth, depth)
		lens := make([]int, depth, depth)
		row_i = 0
		col_i = 0
		t_r_p := row_perms
		t_c_p := col_perms
		l := len(k)
		if l < depth {
			depth = l // Not enough depth of keys
		}
		for i = 0; i < depth; i++ {
			if keys[i], err = strconv.ParseInt(k[i], 10, 64); err != nil {
				return err
			}
			if i < r_leg_l {
				lens[i] = len(r_keys[i])
				t_r_p /= lens[i]
				row_i += int(keys[i]) * t_r_p
			} else {
				lens[i] = len(c_keys[i-r_leg_l])
				t_c_p /= lens[i]
				col_i += int(keys[i]) * t_c_p
			}
		}
		g.Table[row_i+c_leg_l][col_i+r_leg_l] = fmt.Sprintf("%d", v)
		g.Header[row_i+c_leg_l][col_i+r_leg_l] = "dat"
		return
	}

	// Execute the walker
	if err = walker(t.Data, depth, []string{}, table_walker); err != nil {
		return
	}
	if err = walker(t.SubTotals, depth, []string{}, table_walker); err != nil {
		return
	}

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

type SingleDimDef struct {
	Display string
	Key     string
}

func SingleDim(display, key string) DimPartitioner {
	return &SingleDimDef{
		Display: display,
		Key:     key,
	}
}

func (dim *SingleDimDef) Legend() interface{} {
	return dim.Display
}

func (dim *SingleDimDef) GetPartitionMap(data interface{}) (d []interface{}, err error) {
	d = []interface{}{
		dim.Key,
	}
	return
}

func (dim *SingleDimDef) Partition(fact interface{}) (key interface{}, err error) {
	key = dim.Key
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
