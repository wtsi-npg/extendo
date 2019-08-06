package extendo

import (
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

// RodsItem represents both collections and data objects in iRODS. It describes
// the JSON document used by baton and the extendo wrapper.
type RodsItem struct {
	client *Client
	// Local file name
	IFile string `json:"file,omitempty"`
	// Local directory
	IDirectory string `json:"directory,omitempty"`
	// Collection path
	IPath string `json:"collection,omitempty"`
	// Data object name
	IName string `json:"data_object,omitempty"`
	// Data object checksum
	IChecksum string `json:"checksum,omitempty"`
	// Data object size
	ISize uint64 `json:"size,omitempty"`
	// ACLs
	IACLs []ACL `json:"access,omitempty"`
	// Metadata IAVUs
	IAVUs []AVU `json:"avus,omitempty"`
	// Collection contents
	IContents []RodsItem `json:"contents,omitempty"`
	// Data object replicates
	IReplicates []Replicate `json:"replicates,omitempty"`
	// Data object timestamps
	ITimestamps []Timestamp `json:"timestamps,omitempty"`
}

// IsCollection returns true if the item represents a collection.
func (item *RodsItem) IsCollection() bool {
	return item.IName == "" && item.IPath != ""
}

// IsDataObject returns true if the item represents a data object.
func (item *RodsItem) IsDataObject() bool {
	return item.IName != ""
}

// IsLocalDir returns true if the item represents a directory.
func (item *RodsItem) IsLocalDir() bool {
	return item.IFile == "" && item.IDirectory != ""
}

// IsLocalFile returns true if the item represents a file.
func (item *RodsItem) IsLocalFile() bool {
	return item.IFile != ""
}

// RodsPath returns the absolute, cleaned path of the item in iRODS, or the
// empty string.
func (item *RodsItem) RodsPath() (s string) {
	switch {
	case item.IsCollection():
		s = filepath.Clean(item.IPath)
	case item.IsDataObject():
		s = filepath.Clean(filepath.Join(item.IPath, item.IName))
	default:
		s = ""
	}
	return s
}

// RodsPath returns the absolute, cleaned local path of the item, or the
// empty string.
func (item *RodsItem) LocalPath() (s string) {
	switch {
	case item.IsLocalDir():
		s = filepath.Clean(item.IDirectory)
	case item.IsLocalFile():
		s = filepath.Clean(filepath.Join(item.IDirectory, item.IFile))
	default:
		s = ""
	}

	return
}

func (item *RodsItem) String() (s string) {
	switch {
	case item.IsCollection():
		fallthrough
	case item.IsDataObject():
		s = item.RodsPath()
	case item.IsLocalDir():
		fallthrough
	case item.IsLocalFile():
		s = item.LocalPath()
	default:
		s = ""
	}
	return s
}

func Unwrap(items []RodsItem) (RodsItem, error) {
	var item RodsItem

	switch len(items) {
	case 0:
		return item,
		errors.New("cannot unwrap from an empty array")
	case 1:
		return items[0], nil
	default:
		return item,
		errors.Errorf("cannot unwrap from an array with %d members",
			len(items))
	}
}

type RodsItemArr []RodsItem

func (a RodsItemArr) Len() int {
	return len(a)
}

func (a RodsItemArr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a RodsItemArr) Less(i, j int) bool {
	iColl := a[i].IsCollection()
	jColl := a[j].IsCollection()

	switch {
	case iColl && !jColl: // collection / data object
		return true
	case !iColl && jColl: // data object / collection
		return false
	case iColl && jColl: // collection / collection
		return a[i].IPath < a[j].IPath
	case a[i].IPath == a[j].IPath: // data object / data object
		return a[i].IName < a[j].IName
	}

	// data object / data object
	return a[i].IPath < a[j].IPath
}

// ACL is an access control list. Owner may be a user, or more often, a data
// access group.
type ACL struct {
	// Owner isn the iRODS group (or user)
	Owner string `json:"owner"`
	// Level is the iRODS access level (null, read, own etc)
	Level string `json:"level"`
	// Zone is the iRODS zone
	Zone string `json:"zone"`
}

type ACLArr []ACL

func (a ACLArr) Len() int {
	return len(a)
}

func (a ACLArr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ACLArr) Less(i, j int) bool {
	return a[i].Zone < a[j].Zone ||
		a[i].Owner < a[j].Owner ||
		a[i].Level < a[j].Level
}

// AVU is an iRODS attribute, value, units triple.
type AVU struct {
	Attr     string `json:"attribute"`
	Value    string `json:"value"`
	Units    string `json:"units,omitempty"`
	Operator string `json:"operator,omitempty"`
}

type AVUArr []AVU

func (a AVUArr) Len() int {
	return len(a)
}

func (a AVUArr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a AVUArr) Less(i, j int) bool {
	return a[i].Attr < a[j].Attr ||
		a[i].Value < a[i].Value ||
		a[i].Units < a[j].Units
}

// Number is a data object replicate
type Replicate struct {
	// Resource is the resource name where the replicate is located
	Resource string `json:"resource"`
	// Location is the server where the replicate is located
	Location string `json:"location"`
	// Checksum it the checksum of the replicate
	Checksum string `json:"checksum"`
	// Number is iRODS' replicate number
	Number uint16 `json:"replicate"`
	// Valid is iRODS' flag describing whether the replicate is up-to-date
	Valid bool `json:"valid"`
}

type ReplicateArr []Replicate

func (a ReplicateArr) Len() int {
	return len(a)
}

func (a ReplicateArr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ReplicateArr) Less(i, j int) bool {
	return a[i].Resource < a[j].Resource ||
		a[i].Location < a[j].Location ||
		a[i].Number < a[j].Number ||
		a[i].Checksum < a[j].Checksum ||
		(a[i].Valid && !a[j].Valid)
}

type Timestamp struct {
	Created  time.Time `json:"created,omitempty"`
	Modified time.Time `json:"modified,omitempty"`
	Operator string    `json:"operator,omitempty"`
}

type TimestampArr []Timestamp

func (a TimestampArr) Len() int {
	return len(a)
}

func (a TimestampArr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less if a Created time and other time is Zero is Created later, OR
// a Modified time and other time is Zero or is Modified later.
func (a TimestampArr) Less(i, j int) bool {
	ci := !a[i].Created.IsZero() // Is a creation time
	cj := !a[j].Created.IsZero()
	mi := !a[i].Modified.IsZero() // Is a modification time
	mj := !a[j].Modified.IsZero()

	return (ci && !cj) || (!mi && mj) ||
		(ci && cj && a[i].Created.Before(a[j].Created)) ||
		(mi && mj && a[i].Modified.Before(a[j].Created))
}
