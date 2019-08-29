package extendo

import (
	"path/filepath"

	"github.com/pkg/errors"
)

type DataObject struct {
	*RemoteItem
}

// NewDataObject makes a new instance, given a path to an existing data object
// in iRODS.
func NewDataObject(client *Client, remotePath string) (*DataObject, error) {
	remotePath = filepath.Clean(remotePath)
	path := filepath.Dir(remotePath)
	name := filepath.Base(remotePath)

	item, err := client.ListItem(Args{}, RodsItem{IPath: path, IName: name})
	if err != nil {
		return nil, err
	}

	obj := &DataObject{RemoteItem: &RemoteItem{client, &item}}

	return obj, err
}

// PutDataObject make a new instance by sending a file local at localPath
// to remotePath in iRODS. It always uses a forced put operation and
// calculates a server-side checksum. If any slices of AVUs are supplied, they
// are added after the put operation is successful. The returned instance has
// the new checksum fetched to the client.
func PutDataObject(client *Client, localPath string, remotePath string,
	avus ...[]AVU) (*DataObject, error) {
	localPath = filepath.Clean(localPath)
	dir := filepath.Dir(localPath)
	file := filepath.Base(localPath)

	remotePath = filepath.Clean(remotePath)
	path := filepath.Dir(remotePath)
	name := filepath.Base(remotePath)

	item := RodsItem{IDirectory: dir, IFile: file, IPath: path, IName: name}
	putArgs := Args{Force: true, Checksum: true}

	if _, err := client.Put(putArgs, item); err != nil {
		return nil, err
	}

	if len(avus) > 0 {
		var x []AVU
		for _, y := range avus {
			x = append(x, y...)
		}
		item.IAVUs = x

		if _, err := client.MetaAdd(Args{}, item); err != nil {
			return nil, err
		}
	}

	listArgs := Args{Checksum: true}
	if len(avus) > 0 {
		listArgs.AVU = true
	}

	item, err := client.ListItem(listArgs, item)
	if err != nil {
		return nil, err
	}

	obj := &DataObject{RemoteItem: &RemoteItem{client, &item}}

	return obj, err
}

// ArchiveDataObject copies a file to a data object.  The intended use case is
// for when setting a canonical form for the data for long term storage,
// superseding any file and metadata already there.
//
// It differs from PutDataObject in that it always checks the returned checksum
// against the supplied expected checksum argument and returns an error is they
// do not match.
//
// It also differs from PutDataObject in that it uses ReplaceMetadata to
// set metadata, rather than AddMetadata.
func ArchiveDataObject(client *Client, localPath string, remotePath string,
	expectedChecksum string, avus ...[]AVU) (*DataObject, error) {

	obj, err := PutDataObject(client, localPath, remotePath, avus...)
	if err != nil {
		return nil, err
	}

	if obj.Checksum() != expectedChecksum {
		return nil,
			errors.Errorf("failed to archive '%s' to '%s': local "+
				"checksum '%s' did not match remote checksum '%s'",
				localPath, remotePath, expectedChecksum, obj.Checksum())
	}

	var allAVUs []AVU
	for _, x := range avus {
		allAVUs = append(allAVUs, x...)
	}

	err = obj.ReplaceMetadata(UniqAVUs(allAVUs))

	return obj, err
}

func (obj *DataObject) Remove() error {
	_, err := obj.RemoteItem.client.RemObj(Args{}, *obj.RemoteItem.RodsItem)
	return err
}

func (obj *DataObject) Exists() (bool, error) {
	return obj.RemoteItem.Exists()
}

func (obj *DataObject) LocalPath() string {
	return obj.RemoteItem.LocalPath()
}

func (obj *DataObject) RodsPath() string {
	return obj.RemoteItem.RodsPath()
}

func (obj *DataObject) String() string {
	return obj.RemoteItem.String()
}

func (obj *DataObject) ACLs() []ACL {
	return obj.IACLs
}

func (obj *DataObject) FetchACLs() ([]ACL, error) {
	return obj.RemoteItem.FetchACLs()
}

func (obj *DataObject) AddACLs(acls []ACL) error {
	return obj.RemoteItem.AddACLs(acls)
}

func (obj *DataObject) Metadata() []AVU {
	return obj.IAVUs
}

func (obj *DataObject) FetchMetadata() ([]AVU, error) {
	return obj.RemoteItem.FetchMetadata()
}

func (obj *DataObject) AddMetadata(avus []AVU) error {
	return obj.RemoteItem.AddMetadata(avus)
}

func (obj *DataObject) RemoveMetadata(avus []AVU) error {
	return obj.RemoteItem.RemoveMetadata(avus)
}

func (obj *DataObject) ReplaceMetadata(avus []AVU) error {
	return obj.RemoteItem.ReplaceMetadata(avus)
}

func (obj *DataObject) Checksum() string {
	return obj.IChecksum
}

func (obj *DataObject) CalculateChecksum() (string, error) {
	item, err := obj.RemoteItem.client.
		Checksum(Args{Checksum: true, Force: true},
			*obj.RemoteItem.RodsItem)
	if err != nil {
		return "", err
	}
	obj.IChecksum = item.IChecksum

	return obj.IChecksum, err
}

func (obj *DataObject) FetchChecksum() (string, error) {
	checksum, err := obj.RemoteItem.client.
		ListChecksum(*obj.RemoteItem.RodsItem)
	if err != nil {
		return "", err
	}
	obj.IChecksum = checksum

	return obj.IChecksum, err
}

func (obj *DataObject) Replicates() []Replicate {
	return obj.IReplicates
}

func (obj *DataObject) FetchReplicates() ([]Replicate, error) {
	item, err := obj.RemoteItem.client.
		ListItem(Args{Replicate: true}, *obj.RemoteItem.RodsItem)
	if err != nil {
		return []Replicate{}, err
	}
	obj.IReplicates = item.IReplicates

	return obj.IReplicates, err
}

func (obj *DataObject) ValidReplicates() []Replicate {
	return obj.filterReplicates(func(r Replicate) bool {
		return r.Valid
	})
}

func (obj *DataObject) InvalidReplicates() []Replicate {
	return obj.filterReplicates(func(r Replicate) bool {
		return !r.Valid
	})
}

type replicatePred func(r Replicate) bool

func (obj *DataObject) filterReplicates(pred replicatePred) []Replicate {
	var pass []Replicate
	for _, r := range obj.Replicates() {
		if pred(r) {
			pass = append(pass, r)
		}
	}

	return pass
}
