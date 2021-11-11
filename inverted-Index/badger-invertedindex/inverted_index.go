package badgerinvertedindex

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/coocood/freecache"
	"github.com/dgraph-io/badger/v3"
	gocache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"github.com/yatsdb/yatsdb/pkg/metrics"
)

var (
	invertedKeyPrefix = "1$"
	metricKeyPrefix   = "2$"
	sep               = []byte(`\xff`)
)

type DB interface {
	invertedindex.IndexInserter
	invertedindex.IndexMatcher
}

type BadgerIndex struct {
	db           *badger.DB
	batcher      *badgerbatcher.BadgerDBBatcher
	idCache      *freecache.Cache
	metricsCache *gocache.Cache
}

var streamIDCacheVal = []byte("O")

func reloadStreamIDs(db *badger.DB, cache *freecache.Cache) error {
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte(metricKeyPrefix)
		iter := txn.NewIterator(opts)
		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iter.Item().Key()
			if len(key) < len(metricKeyPrefix)+8 {
				return errors.New("metric key format error")
			}
			ID := invertedindex.StreamID(binary.BigEndian.Uint64(key[len(metricKeyPrefix):]))
			cache.SetInt(int64(ID), streamIDCacheVal, 300)
		}
		return nil
	})
}

func NewBadgerIndex(db *badger.DB, batcher *badgerbatcher.BadgerDBBatcher) (*BadgerIndex, error) {
	cache := freecache.NewCache(32 << 20) //32MiB
	if err := reloadStreamIDs(db, cache); err != nil {
		return nil, err
	}
	metricsCache := gocache.New(time.Minute*10, time.Minute*5)

	metrics.StreamIDCacheCount = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "yatsdb",
		Subsystem: "inverted_index",
		Name:      "streamID_cache_entry_count",
		Help:      "total of yatsdb inverted-index streamID cache entry size",
	}, func() float64 {
		return float64(cache.EntryCount())
	})

	metrics.StreamMetricsCacheCount = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "yatsdb",
		Subsystem: "inverted_index",
		Name:      "metrics_cache_entry_count",
		Help:      "total of yatsdb inverted-index metrics cache entry size",
	}, func() float64 {
		return float64(metricsCache.ItemCount())
	})

	return &BadgerIndex{
		db:           db,
		batcher:      batcher,
		idCache:      cache,
		metricsCache: metricsCache,
	}, nil
}

func OpenBadgerIndex(ctx context.Context, path string) (*BadgerIndex, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	go func() {
		for {
			select {
			case <-time.After(time.Minute * 5):
				_ = db.RunValueLogGC(0.5)
			case <-ctx.Done():
				return
			}
		}
	}()
	return NewBadgerIndex(db,
		badgerbatcher.NewBadgerDBBatcher(ctx, 4*1024, db).Start())
}

func (index *BadgerIndex) loadOrStoreStreamID(ID invertedindex.StreamID) bool {
	_, err := index.idCache.GetInt(int64(ID))
	if err != nil {
		index.idCache.SetInt(int64(ID), streamIDCacheVal, 300)
	}

	return err == nil
}

/*
key format: |$0|sep|lable_name|sep|lable_value|stream_id
name sep value sep streamIO
*/
func (index *BadgerIndex) Insert(streamMetric invertedindex.StreamMetric) error {
	if index.loadOrStoreStreamID(streamMetric.StreamID) {
		return nil
	}
	if err := index.update(func(txn *badger.Txn) error {
		var IDBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(IDBuf, uint64(streamMetric.StreamID))
		for _, label := range streamMetric.Labels {
			buf := make([]byte, len(invertedKeyPrefix)+
				len(sep)+len(label.Name)+
				len(sep)+len(label.Value)+
				len(sep)+8)
			key := buf
			//copy name
			n := copy(buf, invertedKeyPrefix)
			buf = buf[n:]
			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy label Name
			n = copy(buf, []byte(label.Name))
			buf = buf[n:]

			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy label value
			n = copy(buf, []byte(label.Value))
			buf = buf[n:]

			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy ID
			n = copy(buf, IDBuf)
			buf = buf[n:]
			if len(buf) > 0 {
				panic("encode label error")
			}
			if err := txn.Set(key, IDBuf); err != nil {
				return err
			}
		}
		data, err := streamMetric.Marshal()
		if err != nil {
			return errors.WithStack(err)
		}
		if err := txn.Set([]byte(string(metricKeyPrefix+string(IDBuf))), data); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		logrus.Errorf("insert metrics %+v failed %+v", streamMetric, err)
		return err
	}
	return nil
}

func (index *BadgerIndex) Matches(labelMatchers ...*prompb.LabelMatcher) ([]invertedindex.StreamMetric, error) {
	var result []invertedindex.StreamMetric
	invertedindex.LabelMatchersSort(labelMatchers)
	err := index.db.View(func(txn *badger.Txn) error {
		matchers := invertedindex.NewMatchers(labelMatchers...)
		firstMatcher := matchers[0]
		if firstMatcher.MatchEmpty &&
			(firstMatcher.LabelsMatcher.Type == labels.MatchEqual || firstMatcher.LabelsMatcher.Type == labels.MatchRegexp) {
			// l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555

			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(metricKeyPrefix)
			iter := txn.NewIterator(opts)
			defer iter.Close()
			for iter.Rewind(); iter.Valid(); iter.Next() {
				var metric invertedindex.StreamMetric
				if err := iter.Item().Value(func(val []byte) error {
					if err := metric.Unmarshal(val); err != nil {
						return errors.WithStack(err)
					}
					return nil
				}); err != nil {
					return err
				}
				if invertedindex.MetricMatches(metric, matchers...) {
					result = append(result, metric)
				}
			}
			return nil
		} else {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(invertedKeyPrefix + string(sep) + firstMatcher.LabelsMatcher.Name)

			if firstMatcher.LabelsMatcher.Type == labels.MatchEqual {
				opts.Prefix = []byte(invertedKeyPrefix + string(sep) +
					firstMatcher.LabelsMatcher.Name + string(sep) +
					firstMatcher.LabelsMatcher.Value + string(sep))
				matchers = matchers[1:]
			}
			var streamIDs []invertedindex.StreamID
			iter := txn.NewIterator(opts)
			defer iter.Close()
			for iter.Rewind(); iter.Valid(); iter.Next() {
				if err := iter.Item().Value(func(val []byte) error {
					if len(val) != 8 {
						return errors.Errorf("value size %d error", len(val))
					}
					streamIDs = append(streamIDs, invertedindex.StreamID(binary.BigEndian.Uint64(val)))
					return nil
				}); err != nil {
					return err
				}
			}

			for _, streamID := range streamIDs {
				var buffer [8]byte
				binary.BigEndian.PutUint64(buffer[:], uint64(streamID))

				if obj, ok := index.metricsCache.Get(string(buffer[:])); ok {
					if invertedindex.MetricMatches(obj.(invertedindex.StreamMetric), matchers...) {
						result = append(result, obj.(invertedindex.StreamMetric))
					}
				} else {
					item, err := txn.Get([]byte(metricKeyPrefix + string(buffer[:])))
					if err != nil {
						return errors.WithStack(err)
					}
					var metric invertedindex.StreamMetric
					if err := item.Value(func(val []byte) error {
						if err := metric.Unmarshal(val); err != nil {
							return errors.WithStack(err)
						}
						return nil
					}); err != nil {
						return err
					}
					if invertedindex.MetricMatches(metric, matchers...) {
						result = append(result, metric)
						//add to cache
						index.metricsCache.Add(string(buffer[:]), metric, time.Minute*5)
					}
				}
			}
			return nil
		}
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (index *BadgerIndex) update(fn func(txn *badger.Txn) error) error {
	var errs = make(chan error, 1)
	index.batcher.Update(badgerbatcher.BadgerOP{
		Op: fn,
		Commit: func(err error) {
			errs <- err
		},
	})
	return <-errs
}
