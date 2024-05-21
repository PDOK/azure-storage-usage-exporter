package agg

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/du"
	"github.com/stretchr/testify/require"
)

func TestAggregator_Aggregate(t *testing.T) {
	someFixedTime, _ := time.Parse(time.DateOnly, "2024-04-20")
	type fields struct {
		duReader           du.Reader
		labelsWithDefaults Labels
		rules              []AggregationRule
	}
	type args struct {
		previousRunDate time.Time
	}
	tests := []struct {
		name                   string
		fields                 fields
		args                   args
		wantAggregationResults []AggregationResult
		wantRunDate            time.Time
		wantErr                bool
	}{{
		name: "basic",
		fields: fields{
			duReader: &fakeDuReader{
				runDate: someFixedTime,
				rows: []du.Row{
					{Dir: "dir1/dir2", Deleted: boolPtr(false), Bytes: 100, Count: 12},
					{Dir: "unallocatable", Deleted: boolPtr(false), Bytes: 666, Count: 666},
					{Dir: "dir1/dir2", Deleted: boolPtr(true), Bytes: 200, Count: 30},
					{Dir: "special/delivery", Deleted: boolPtr(false), Bytes: 321, Count: 1},
				},
			},
			labelsWithDefaults: Labels{
				"level1": "default1",
				"level2": "default2",
			},
			rules: []AggregationRule{
				{Pattern: NewReGroup(`^(?P<level1>special)(/|$)`), StaticLabels: Labels{"level2": "sauce"}},
				{Pattern: NewReGroup(`^(?P<level1>[^/]+)/(?P<level2>[^/]+)`), StaticLabels: Labels{}},
			},
		},
		args: args{
			previousRunDate: someFixedTime.Add(-24 * time.Hour),
		},
		wantAggregationResults: []AggregationResult{
			{AggregationGroup: AggregationGroup{Labels: Labels{"level1": "default1", "level2": "default2"}, Deleted: false}, StorageUsage: 666},
			{AggregationGroup: AggregationGroup{Labels: Labels{"level1": "special", "level2": "sauce"}, Deleted: false}, StorageUsage: 321},
			{AggregationGroup: AggregationGroup{Labels: Labels{"level1": "dir1", "level2": "dir2"}, Deleted: true}, StorageUsage: 200},
			{AggregationGroup: AggregationGroup{Labels: Labels{"level1": "dir1", "level2": "dir2"}, Deleted: false}, StorageUsage: 100},
		},
		wantRunDate: someFixedTime,
		wantErr:     false,
	}, {
		name: "error starting to read",
		fields: fields{
			duReader: &fakeDuReader{
				runDate:          someFixedTime,
				errorImmediately: true,
			},
		},
		args: args{
			previousRunDate: someFixedTime.Add(-24 * time.Hour),
		},
		wantRunDate: time.Time{},
		wantErr:     true,
	}, {
		name: "error while reading",
		fields: fields{
			duReader: &fakeDuReader{
				runDate:        someFixedTime,
				errorInChannel: true,
			},
		},
		args: args{
			previousRunDate: someFixedTime.Add(-24 * time.Hour),
		},
		wantRunDate: someFixedTime,
		wantErr:     true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := NewAggregator(tt.fields.duReader, tt.fields.labelsWithDefaults, tt.fields.rules)
			require.Nil(t, err)
			gotAggregationResults, gotRunDate, err := a.Aggregate(tt.args.previousRunDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("Aggregate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotAggregationResults, tt.wantAggregationResults) {
				t.Errorf("Aggregate() gotAggregationResults = %v, want %v", gotAggregationResults, tt.wantAggregationResults)
			}
			if !reflect.DeepEqual(gotRunDate, tt.wantRunDate) {
				t.Errorf("Aggregate() gotRunDate = %v, want %v", gotRunDate, tt.wantRunDate)
			}
		})
	}
}

type fakeDuReader struct {
	runDate          time.Time
	rows             []du.Row
	errorImmediately bool
	errorInChannel   bool
}

func (f *fakeDuReader) Read(previousRunDate time.Time) (time.Time, <-chan du.Row, <-chan error, error) {
	if f.errorImmediately {
		return time.Time{}, nil, nil, errors.New("error starting to read")
	}
	if !f.runDate.After(previousRunDate) {
		return f.runDate, nil, nil, errors.New("last run date is not after previous run date")
	}
	rowsCh := make(chan du.Row)
	errCh := make(chan error)
	go func() {
		for _, row := range f.rows {
			rowsCh <- row
		}
		if f.errorInChannel {
			errCh <- errors.New("error while reading")
		}
		close(rowsCh)
		close(errCh)
	}()
	return f.runDate, rowsCh, errCh, nil
}

func boolPtr(b bool) *bool {
	return &b
}
