package label2db

import "github.com/prometheus/prometheus/tsdb/fileutil"


type LabelValue struct {
	Value StringID
	IDSet []uint64
}

type LabelSet struct {
	Label  StringID
	Values map[StringID]LabelValue
}

type MemTable struct {
	LabelSets map[StringID]LabelSet
}

