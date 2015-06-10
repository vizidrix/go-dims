package dims

import (
	//"bytes"
	"errors"
	"fmt"
	"log"
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
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	g = &TableReportGridViewModel{}
	defer func() {
		log.Printf("\n\nGrid")
		for _, r := range g.Table {
			log.Printf("%#v", r)
		}
	}()
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

	/*
		for i = 0; i < r_leg_l; i++ {
			for j = 0; j < c_leg_l; j++ {
				g.Header[i][j] = "row"
			}
		}
	*/

	log.Printf("Subtotals: %#v", t.SubTotals)
	// Map row labels
	t_perm_l = row_perms
	for i, k := range r_keys {
		k_l := len(k)
		t_perm_l = t_perm_l / k_l
		for j = 0; j < row_perms; j++ {
			g.Table[j+c_leg_l][i] = k[(j/t_perm_l)%k_l]
			g.Header[j+c_leg_l][i] = "row"
			//g.Table[j+c_leg_l][i] = t.
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
		}
	}

	/*
		// Clear
		for i = 0; i < r_leg_l; i++ {
			for j = 0; j < c_leg_l; j++ {
				g.Header[i][j] = ""
			}
		}
	*/

	// Fill default values
	for i = 0; i < row_perms; i++ {
		for j = 0; j < col_perms; j++ {
			g.Table[i+c_leg_l][j+r_leg_l] = "-"
		}
	}

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
			log.Printf("[%#v] data [%#v]", s[l-d:], data)
			err = f(s[l-d:], data)
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
	//log.Printf("PartitionMap [ %#v ]", keys)
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

/*
	var err error
	var k_i = 0
	var r_i int64 = 0
	row_i = 0
	col_i = 0

	for i = 0; i < r_leg_l; i++ {
		if r_i, err = strconv.ParseInt(k[k_i], 10, 64); err != nil {
			log.Printf("Bad parse in map [ %#v ]", k)
			return err
		}
		//row_i += k_l + r_i // Account for prev
		//k_l = len(r_keys[i])
		log.Printf("k_l / r_i [ %d / %d ]", depth, r_i)
		k_i++
	}
*/
//for _, s := range k {

//}

/*
	var ok bool
	var m map[string]interface{}
	if m, ok = t.Data.(map[string]interface{}); !ok {
		return // Require at least one dimension
	}
	var d = r_leg_l + c_leg_l // Depth of maps
	m_path := make([]map[string]interface{}, 0, d)
	m_path = append(m_path, m)

	for k, v := range m_path[len(m_path)-1] {
		log.Printf("k [ %s ]", k)

	}
	for d := 0; d < r_leg_l + c_leg_l; d++ {
		for k, v := range m_path[]
	}
	log.Printf("map [ %#v ]", m)
*/
/*
	//r_map := make([]string, 0, r_leg_l)
	//c_map := make([]string, 0, c_leg_l)
	path := make([]string, 0, r_leg_l*c_leg_l)
	//m_path := make([]map[string]interface{}, 0, r_leg_l+c_leg_l)
	//m_path = append(m_path, m)
	//var t_map map[string]interface{}
	//var t_val interface{}
	r_perm_l := row_perms
	for r_i, r_k := range r_keys {
		//r_map = append(r_map, make(map[string]interface{}))
		r_k_l := len(r_k)
		r_loops := row_perms / r_perm_l
		r_perm_l = r_perm_l / r_k_l
		for r_p := 0; r_p < r_perm_l; r_p++ {
			for r_loop := 0; r_loop < r_loops; r_loop++ {
				for r_j := 0; r_j < r_k_l; r_j++ {
					path = append(path, fmt.Sprintf("%d", r_j))
					log.Printf("\npath [ %#v ]", path)
					row_i = (((r_j + (r_loop * (r_i * r_k_l)))) + r_p)// + c_leg_l
					log.Printf("r [ %d ]", row_i)
					c_perm_l := col_perms
					for c_i, c_k := range c_keys {
						c_k_l := len(c_k)
						c_loops := col_perms / c_perm_l
						c_perm_l = c_perm_l / c_k_l
						for c_p := 0; c_p < c_perm_l; c_p++ {
							for c_loop := 0; c_loop < c_loops; c_loop++ {
								for c_j := 0; c_j < c_k_l; c_j++ {
									path = append(path, fmt.Sprintf("%d", c_j))
									log.Printf("\npath [ %#v ]", path)
									col_i = (((c_j + (c_loop * (c_i * c_k_l))) * (c_k_l-1)) + c_p)// + r_leg_l
									log.Printf("row/col [ %d - %d / %d - %d ]", r_i, row_i, c_i, col_i)
									//path = path[:len(path)-1] // Pop
									//log.Printf("\npath [ %#v ]", path)
								}
							}
						}
						path = path[:len(path)-c_k_l] // Pop
						log.Printf("\npath [ %#v ]", path)
					}

					//path = path[:len(path)-1] // Pop
					//log.Printf("\npath [ %#v ]", path)
				}
			}
		}
	}
*/
//	return
//}

//var ok bool
//var r_k string
//var c_k string
//var m map[string]interface{}
//var r_t map[string]interface{}
//var c_t map[string]interface{}
//if m, ok = t.Data.(map[string]interface{}); !ok {
//	log.Printf("Invalid map")
//	return
//}

/*
	row_i, col_i = 0, 0
	path := make([]string, 0, r_leg_l * c_leg_l)
	//for i, r_key := range r_keys {
	for _, r_key := range r_keys {
		r_l := len(r_key)
		for r_i := 0; r_i < r_l; r_i++ {
			path = append(path, fmt.Sprintf("%d", r_i))
			//for j, c_key := range c_keys {
			for _, c_key := range c_keys {
				c_l := len(c_key)
				for c_i := 0; c_i < c_l; c_i++ {
					//log.Printf("\ni, r_i [ %d, %d ] j, c_i [ %d, %d ]", i, r_i, j, c_i)
					//log.Printf("\nr/c [ %d / %d ]", row_i, col_i)
					g.Table[row_i + c_leg_l][col_i + r_leg_l] = "-"
					log.Printf("r/c [ %d / %d ]", row_i + c_leg_l, col_i + r_leg_l)
					col_i++
				}
			}
			row_i++
			col_i = 0
		}
	}
*/

/*
	row_i, col_i = 0, 0
	path := make([]string, 0, r_leg_l * c_leg_l)
	//for i, r_key := range r_keys {
	for _, r_key := range r_keys {
		r_l := len(r_key)
		for r_i := 0; r_i < r_l; r_i++ {

			//for j, c_key := range c_keys {
			for _, c_key := range c_keys {
				c_l := len(c_key)
				for c_i := 0; c_i < c_l; c_i++ {
					//log.Printf("\ni, r_i [ %d, %d ] j, c_i [ %d, %d ]", i, r_i, j, c_i)
					//log.Printf("\nr/c [ %d / %d ]", row_i, col_i)
					g.Table[row_i + c_leg_l][col_i + r_leg_l] = "-"
					log.Printf("r/c [ %d / %d ]", row_i + c_leg_l, col_i + r_leg_l)
					col_i++
				}
			}
			row_i++
			col_i = 0
		}
	}
*/
/*
	var m map[string]interface{}
	var ok bool
	if m, ok = t.Data.(map[string]interface{}); !ok {
		log.Printf("\nno data, check for value")
		return
	}
	var k string
	var v interface{}
	for {
		for k, v = range m {
			log.Printf("\nk [ %s ] v [ %#v ]", k, v)
		}
	}
*/
/*
	for {
		for
		if m, ok = m.(map[string]interface{}); !ok {
			log.Printf("no data, check for value")
			break
		} else {
			log.Printf("got data [ %#v ]", m)
		}
	}
*/
/*
	log.Printf("\n\nrows\n%#v\n\n- map\n%#v", r_keys, m)
	row_i, col_i = 0, 0
	for i, r_key := range r_keys {
		//r_t = m
		r_l := len(r_key)
		for r_i := 0; r_i < r_l; r_i++ {
			r_k = fmt.Sprintf("%d", r_i)
			if r_t, ok = m[r_k].(map[string]interface{}); !ok {
				log.Printf("\n\nRow key miss [ %d ]", r_i)
				for j, c_key := range c_keys {
					c_l := len(c_key)
					for c_i := 0; c_i < c_l; c_i++ {
						log.Printf("\nCLEAR --> i, r_i [ %d, %d ] j, c_i [ %d, %d ]", i, r_i, j, c_i)
						//row_i = r_i * (i * r_l)
						//col_i = c_i * (j * c_l)
						log.Printf("\n\tr/c [ %d / %d ]", row_i, col_i)
						g.Table[row_i][col_i] = "-"
						col_i++
					}
				}
			} else { // Found row key match
				log.Printf("\n\nRow key hit [ %d ]", r_i)
				log.Printf("\n\nr_t [ %#v ]", r_t)
				c_t = r_t
				for j, c_key := range c_keys {
					c_l := len(c_key)
					for c_i := 0; c_i < c_l; c_i++ {
						c_k = fmt.Sprintf("%d", c_i)
						//if c_t, ok = m[c_k].(map[string]interface{}); !ok {
						if c_t, ok = c_t[c_k].(map[string]interface{}); !ok {
							log.Printf("\n\nCol key miss [ %d ]", c_i)
							log.Printf("\nCLEAR --> i, r_i [ %d, %d ] j, c_i [ %d, %d ]", i, r_i, j, c_i)
							log.Printf("\n\tr/c [ %d / %d ]", row_i, col_i)
							g.Table[row_i][col_i] = "-"
						} else { // Found col key match
							log.Printf("\n\nCol key hit [ %d ]", c_i)
							log.Printf("\n\nc_t [ %#v ]", c_t)
							log.Printf("i, r_i [ %d, %d ] j, c_i [ %d, %d ]", i, r_i, j, c_i)
							log.Printf("\n\tr/c [ %d / %d ]", row_i, col_i)
							//g.Table[i][j]

						}
						log.Printf("\n\nFINAL c_t [ %#v ]", c_t)
						//if c_f, ok = ct
						//g.Table[row_i][col_i] =
						col_i++
					}
				}
			}
			row_i++
			col_i = 0
		}
		log.Printf("Finished row key [ %d ]", i)
	}
*/
/*
	//for i = 0; i < c_key_l; i++ {
	for i = 0; i < len(c_key[i]); i++ {
		g.Table[i + r_leg_l] = c_key[col_i][j]
	}
*/
//	col_i++
//}

/*
func T() {
	var g *TableReportGridViewModel
	var t *TableReportViewModel
	var r, c = 0, 1
	var i, j = 0, 0
	var r_legs, c_legs = t.Legends[r], t.Legends[c]
	var r_keys, c_keys = t.Keys[r], t.Keys[c]
	var r_leg_l, c_leg_l = 0, 0
	var r_key_l, c_key_l = 0, 0
	var r_dat_l, c_dat_l = 0, 0
	for i = 0; i < len(r_legs); i++ {
		r_leg_l *= len(r_legs[i])
		r_key_l = len(r_keys[i])
	}
	for i = 0; i < len(c_legs); i++ {
		c_leg_l *= len(c_legs[i])
		c_key_l = len(c_keys[i])
	}

	log.Printf("\nr/c - \nleg_l [ %d / %d ]\nleg [ \n%#v\n%#v ]\nkey_l [ %d / %d ]\nkey [ \n%#v\n%#v ]\n\n\n", r_leg_l, c_leg_l, r_legs, c_legs, r_key_l, c_key_l, r_keys, c_keys)

	var rows = c_leg_l + r_key_l + r_leg_l + 1;
	var cols = r_leg_l + c_key_l + c_leg_l + 1;

	g = &TableReportGridViewModel{
		Table: make([][]string, rows, rows),
	}
	for i = 0; i < rows; i++ {
		g.Table[i] = make([]string, cols, cols)
	}


	for i = 0; i < len(r_legs); i++ {
		i_l := len(r_legs[i])
		for i_d := 0; i_d < i_l; i_d++ {
			log.Printf("[%d * %d]", i, i_d)
		//	g.Table[i*i_d]
		}
	}


	log.Printf("Grid [ %#v ]", g)
}
*/
/*
	for r_leg_i = 0; r_leg_i < r_leg_l; r_leg_i++ {
		for c_leg_i = 0; c_leg_i < c_leg_l; c_leg_i++ {
			g.Table[i + c_leg_l][j + r_leg_l] = fmt.Sprintf("%d%d", r_keys[i], c_keys[j])
		}
	}
*/
