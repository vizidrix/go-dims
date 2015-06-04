package dims

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
)

func ErrInvalidPartition(p interface{}) error {
	return errors.New(fmt.Sprintf("Invalid partition [ %#v ]", p))
}

type DimPartitioner interface {
	Display() string
	GetPartitions(data interface{}) (u []string, d [][]string, err error)
	Partition(data interface{}) (key string, value int64, err error)
}

type OneDimReport struct {
	Legend    string
	UnionKeys []string
	Keys      []string
	Data      []int64
	Total     int64
}

func BuildOneDimReport(data interface{}, dim DimPartitioner) (r *OneDimReport, err error) {
	var ps []string
	if ps, _, err = dim.GetPartitions(data); err != nil {
		return
	}
	l := len(ps)
	r = &OneDimReport{
		Legend:    dim.Display(),
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

type TwoDimReport struct {
	Legend        []string
	Dim1UnionKeys []string
	Dim1Keys      [][]string
	Dim2UnionKeys []string
	Dim2Keys      [][]string
	Data          [][]int64
	Dim1Totals    []int64
	Dim2Totals    []int64
	Total         int64
}

func BuildTwoDimGridReport(data interface{}, dim1, dim2 DimPartitioner) (r *TwoDimReport, err error) {
	r = &TwoDimReport{
		Legend: []string{
			dim1.Display(),
			dim2.Display(),
		},
	}
	if r.Dim1UnionKeys, r.Dim1Keys, err = dim1.GetPartitions(data); err != nil {
		return
	}
	if r.Dim2UnionKeys, r.Dim2Keys, err = dim2.GetPartitions(data); err != nil {
		return
	}
	l1 := len(r.Dim1UnionKeys)
	l2 := len(r.Dim2UnionKeys)
	r.Data = make([][]int64, l1, l1)
	for i := 0; i < l1; i++ {
		r.Data[i] = make([]int64, l2, l2)
	}
	r.Dim1Totals = make([]int64, l1, l1)
	r.Dim2Totals = make([]int64, l2, l2)
	data_map := make(map[string]map[string]int64)
	for _, du1 := range r.Dim1UnionKeys {
		data_map[du1] = make(map[string]int64)
		for _, du2 := range r.Dim2UnionKeys {
			data_map[du1][du2] = 0
		}
	}
	err = MapSlice(data, func(fact interface{}) error {
		k1, v1, err := dim1.Partition(fact)
		if err != nil {
			return err
		}
		k2, v2, err := dim2.Partition(fact)
		if err != nil {
			return err
		}
		if v1 != v2 {
			return errors.New("Invalid values in facts")
		}
		data_map[k1][k2] += v1
		return nil
	})
	for i, p1 := range r.Dim1UnionKeys {
		for j, p2 := range r.Dim2UnionKeys {
			v := data_map[p1][p2]
			r.Data[i][j] = v
			r.Dim1Totals[i] += v
			r.Dim2Totals[j] += v
			r.Total += v
		}
	}
	return
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

type MergeDimDef struct {
	separator string
	Dims      []DimPartitioner
}

func MergeDim(separator string, dims ...DimPartitioner) *MergeDimDef {
	return &MergeDimDef{
		Dims: dims,
	}
}

func (dim *MergeDimDef) Display() string {
	l := len(dim.Dims)
	buf := &bytes.Buffer{}
	for i := 0; i < l; i++ {
		if i > 0 {
			buf.WriteString(dim.separator)
		}
		buf.WriteString(dim.Dims[i].Display())
	}
	return buf.String()
}

func (dim *MergeDimDef) GetPartitions(data interface{}) (u []string, d [][]string, err error) {
	return MapPartitions(data, dim.Dims...)
}

func (dim *MergeDimDef) Partition(fact interface{}) (key string, value int64, err error) {
	l := len(dim.Dims)
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
			fact := s.Index(i)
			if fact.Kind() == reflect.Ptr {
				fact = fact.Elem()
			}
			if err = f(fact.Interface()); err != nil {
				return err
			}
		}
	default:
		err = errors.New("Value was not a slice")
	}
	return
}

type ByAlpha []string

func (s ByAlpha) Len() int {
	return len(s)
}
func (s ByAlpha) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByAlpha) Less(i, j int) bool {
	return s[i] < s[j]
}
