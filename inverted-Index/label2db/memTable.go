package label2db

import (
	"sort"
	"sync"

	"github.com/google/btree"
)

type Cow struct {
	_ int32
}

type IDSet []uint64
type LabelValue struct {
	cow   *Cow
	value string
	IDSet IDSet
}

type LabelSet struct {
	cow    *Cow
	label  string
	Values *btree.BTree
}

type MemTable struct {
	cow       *Cow
	labelSets *btree.BTree
}

func (is *IDSet) Len() int {
	return len(*is)
}

func (is *IDSet) Less(i, j int) bool {
	return (*is)[i] < (*is)[j]
}

func (is *IDSet) Swap(i, j int) {
	(*is)[i], (*is)[j] = (*is)[j], (*is)[i]
}

func (ls *LabelSet) Less(item btree.Item) bool {
	return ls.label < item.(*LabelSet).label
}

func (ls *LabelValue) Less(item btree.Item) bool {
	return ls.value < item.(*LabelValue).value
}

func (mt *MemTable) loadOrCreateLabelValue(btree *btree.BTree, value string) *LabelValue {
	item := btree.Get(&LabelValue{
		value: value,
	})
	if item == nil {
		node := &LabelValue{
			cow:   mt.cow,
			value: value,
		}
		btree.ReplaceOrInsert(node)
		return node
	}
	return item.(*LabelValue)
}

func (mt *MemTable) loadOrCreateLabelSet(label string) *LabelSet {
	item := mt.labelSets.Get(&LabelSet{
		label: label,
	})
	if item == nil {
		lValue := &LabelSet{
			cow:    mt.cow,
			label:  label,
			Values: btree.New(2),
		}
		mt.labelSets.ReplaceOrInsert(lValue)
		return lValue
	} else {
		return item.(*LabelSet)
	}
}

func (ls *LabelSet) Range(fn func(lVal *LabelValue) bool) {
	ls.Values.Ascend(func(i btree.Item) bool {
		return fn(i.(*LabelValue))
	})
}
func (ls *LabelSet) GetLabelValue(value string) *LabelValue {
	lval := newLabelValue(value)
	defer releaseLabelValue(lval)
	item := ls.Values.Get(lval)
	if item == nil {
		return nil
	} else {
		return item.(*LabelValue)
	}
}

func (mt *MemTable) GetLabelSetNode(key string) *LabelSet {
	lSet := newLabelSet(key)
	defer releaseLabelSet(lSet)
	item := mt.labelSets.Get(lSet)
	if item == nil {
		return nil
	} else {
		return item.(*LabelSet)
	}
}

func (mt *MemTable) append(key string, value string, id uint64) {
	lSet := mt.loadOrCreateLabelSet(key)
	if lSet.cow != mt.cow {
		lSet.cow = mt.cow
		lSet.Values = lSet.Values.Clone()
	}
	lVal := mt.loadOrCreateLabelValue(lSet.Values, value)
	if lVal.cow != mt.cow {
		lVal.IDSet = append(make([]uint64, 0, int(float64(len(lVal.IDSet))*1.3)), lVal.IDSet...)
	}
	lVal.IDSet = append(lVal.IDSet, id)
	if !sort.IsSorted(&lVal.IDSet) {
		sort.Sort(&lVal.IDSet)
	}
}

func (mt *MemTable) Clone() *MemTable {
	cow1 := *mt.cow
	cow2 := *mt.cow
	mt.cow = &cow1
	return &MemTable{
		cow:       &cow2,
		labelSets: mt.labelSets.Clone(),
	}
}

var labelSetObjPool = sync.Pool{
	New: func() interface{} {
		return new(LabelSet)
	},
}

func releaseLabelSet(lset *LabelSet) {
	lset.label = ""
	labelSetObjPool.Put(lset)
}
func newLabelSet(label string) *LabelSet {
	obj := labelSetObjPool.Get().(*LabelSet)
	obj.label = label
	return obj
}

var labelValueObjPool = sync.Pool{
	New: func() interface{} {
		return new(LabelValue)
	},
}

func releaseLabelValue(lset *LabelValue) {
	lset.value = ""
	labelValueObjPool.Put(lset)
}
func newLabelValue(val string) *LabelValue {
	obj := labelValueObjPool.Get().(*LabelValue)
	obj.value = val
	return obj
}
