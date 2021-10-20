package yatsdb

import (
	"os"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

func TestOpenTSDB(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args: args{
				options: Options{
					BadgerDBStoreDir:   t.Name() + "/badgerdbstore",
					FileStreamStoreDir: t.Name() + "/fileStreamStore",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenTSDB(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenTSDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("OpenTSDB nil")
			}
		})
	}
}

func Test_tsdb_WriteSamples(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	t.Cleanup(func() {
		//	os.RemoveAll(t.Name())
	})

	tsdb, err := OpenTSDB(Options{
		BadgerDBStoreDir:   t.Name() + "/badgerdbstore",
		FileStreamStoreDir: t.Name() + "/fileStreamStore",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	type args struct {
		request *prompb.WriteRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args: args{
				request: &prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "2"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tsdb.WriteSamples(tt.args.request); (err != nil) != tt.wantErr {
				t.Errorf("tsdb.WriteSamples() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}

func Test_tsdb_ReadSimples(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	tsdb, err := OpenTSDB(Options{
		BadgerDBStoreDir:   t.Name() + "/badgerdbstore",
		FileStreamStoreDir: t.Name() + "/fileStreamStore",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = tsdb.WriteSamples(&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "2"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
	}})
	if err != nil {
		t.Fatal(err.Error())
	}

	type args struct {
		req *prompb.ReadRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *prompb.ReadResponse
		wantErr bool
	}{
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						&prompb.Query{
							Matchers: []*prompb.LabelMatcher{
								&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   5,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					&prompb.QueryResult{
						Timeseries: []*prompb.TimeSeries{
							&prompb.TimeSeries{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							&prompb.TimeSeries{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							&prompb.TimeSeries{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tsdb.ReadSimples(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("tsdb.ReadSimples() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tsdb.ReadSimples() = %v, want %v", got, tt.want)
			}
		})
	}
}
