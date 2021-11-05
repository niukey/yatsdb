package invertedindex

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

func MetricsMatches(metrics []StreamMetric, matchers ...*Matcher) []StreamMetric {
	var result = make([]StreamMetric, 0, len(metrics)/2)
	for _, metric := range metrics {
		if MetricMatches(metric, matchers...) {
			result = append(result, metric)
		}
	}
	return result
}

func MetricMatches(metric StreamMetric, matchers ...*Matcher) bool {
	for _, matcher := range matchers {
		if !matcher.Matches(metric) {
			return false
		}
	}
	return true
}

type Matcher struct {
	MatchEmpty    bool
	LabelsMatcher *labels.Matcher
}

func NewMatchers(labelMatchers ...*prompb.LabelMatcher) []*Matcher {
	var matchers []*Matcher
	for _, labelMatcher := range labelMatchers {
		matchers = append(matchers, NewMatcher(labelMatcher))
	}
	return matchers
}

func NewMatcher(labelMatcher *prompb.LabelMatcher) *Matcher {
	matcher := &Matcher{
		LabelsMatcher: labels.MustNewMatcher(labels.MatchType(labelMatcher.Type),
			labelMatcher.Name, labelMatcher.Value),
	}
	matcher.MatchEmpty = matcher.LabelsMatcher.Matches("")
	return matcher
}

func (matcher *Matcher) Matches(metric StreamMetric) bool {
	switch matcher.LabelsMatcher.Type {
	case labels.MatchEqual:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.LabelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.LabelsMatcher.Matches(label.Value) {
					return true
				}
			}
		}
		// l=""
		// If the matchers for a labelname selects an empty value, it selects all
		// the series which don't have the label name set too. See:
		// https://github.com/prometheus/prometheus/issues/3575 and
		// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
		return matcher.MatchEmpty && !findLabel
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.LabelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.LabelsMatcher.Matches(label.Value) {
					continue
				}
				return false
			}
		}
		return findLabel || matcher.MatchEmpty
	case labels.MatchRegexp:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.LabelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.LabelsMatcher.Matches(label.Value) {
					return true
				}
			}
		}
		return !findLabel && matcher.MatchEmpty
	}
	return false
}
