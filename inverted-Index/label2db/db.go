package label2db

import (
	"io"
	"sort"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type DB struct {
	mtables       *MemTable
	flushingTable *MemTable
	SegmentTables []SegmetTable
}

func SortMatchers(matcher []*invertedindex.Matcher) {
	sort.SliceStable(matcher, func(i, j int) bool {
		return !matcher[i].MatchEmpty
	})
}

type ReadTxn struct {
	mtables       *MemTable
	flushingTable *MemTable
	SegmentTables []SegmetTable
}

func (txn *ReadTxn) CollectionIDs(matcher *labels.Matcher) ([]uint64, error) {
	var idSet []uint64

	for _, mtable := range []*MemTable{txn.mtables, txn.flushingTable} {
		lSet := mtable.GetLabelSetNode(matcher.Name)
		if lSet == nil {
			continue
		}
		if matcher.Type == labels.MatchEqual {
			lVal := lSet.GetLabelValue(matcher.Value)
			if lVal == nil {
				continue
			}
			idSet = append(idSet, lVal.IDSet...)
		} else {
			lSet.Range(func(lVal *LabelValue) bool {
				if matcher.Matches(lVal.value) {
					idSet = append(idSet, lVal.IDSet...)
				}
				return true
			})
		}
	}

	for _, segment := range txn.SegmentTables {
		lSetNode, err := segment.GetLabelSetNode([]byte(matcher.Name))
		if err != nil {
			return nil, err
		}
		if matcher.Type == labels.MatchEqual {
			valueNode, err := segment.findLabelValueNode(lSetNode.Start, []byte(matcher.Value))
			if err != nil {
				return nil, err
			}
			idSet = append(idSet, segment.GetValueNodeIDs(valueNode)...)
		} else {
			iter := segment.LabelValueNodeIteractor(lSetNode)
			for {
				node, err := iter.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}
				if matcher.Matches(string(segment.GetValueNodeLabel(node))) {
					idSet = append(idSet, segment.GetValueNodeIDs(node)...)
				}
			}
		}
	}
	return idSet, nil
}

func (db *DB) Matches(labelMatchers ...*prompb.LabelMatcher) ([]invertedindex.StreamMetric, error) {
	var result []invertedindex.StreamMetric
	matchers := invertedindex.NewMatchers(labelMatchers...)
	SortMatchers(matchers)

	return result, nil

}
