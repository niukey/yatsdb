package invertedindex

import "github.com/prometheus/prometheus/prompb"

type IndexInserter interface {
	Insert(streamMetric StreamMetric) error
}

type IndexMatcher interface {
	Matches(matcher ...*prompb.LabelMatcher) ([]StreamMetric, error)
}
