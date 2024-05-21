// Package du is the link between cloud storage and du (disk usage) data
package du

import "time"

// StorageUsage is storage usage/size in bytes
type StorageUsage = int64

// Row is info about the aggregated size of a specific dir (or prefix if you will) in cloud storage
type Row struct {
	Dir     string       `db:"dir"`
	Deleted *bool        `db:"deleted"`
	Bytes   StorageUsage `db:"bytes"`
	Count   int64        `db:"cnt"`
}

// Reader provides Row s from a cloud storage provider
//
// The runDate indicates the actuality of the data.
// If there is no new data, the returned runDate will be the same and the channel nil.
type Reader interface {
	Read(previousRunDate time.Time) (runDate time.Time, rows <-chan Row, errs <-chan error, err error)
}
