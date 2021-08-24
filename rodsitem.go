/*
 * Copyright (C) 2019, 2020, 2021. Genome Research Ltd. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License,
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @file rodsitem.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"path/filepath"
	"sort"
	"strings"
	"time"

	logs "github.com/wtsi-npg/logshim"
)

// RodsItem represents both collections and data objects in iRODS. It describes
// the JSON document used by baton and the extendo wrapper and its purpose is to
// enable extendo to communicate with baton. Extendo includes a higher level API
// having distinct types for data objects and collections which should be used
// in preference to RodsItem. A RodsItem is not safe for concurrent use.
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

// Exists returns true if the item exists in iRODS, or false otherwise.
func (item *RodsItem) Exists() (bool, error) {
	_, err := item.client.ListItem(Args{}, *item)
	if err != nil {
		if IsRodsError(err) {
			code, cerr := RodsErrorCode(err)
			if cerr == nil && code == RodsUserFileDoesNotExist {
				return false, nil
			}

			return false, err // Return original error
		}

		return false, err
	}

	return true, nil
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

// LocalPath returns the absolute, cleaned local path of the item, or an
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

// String returns a string representation of the item.
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

func (item *RodsItem) ACLs() []ACL {
	return item.IACLs
}

func (item *RodsItem) FetchACLs() ([]ACL, error) {
	it, err := item.client.ListItem(Args{ACL: true}, *item)
	if err != nil {
		return []ACL{}, err
	}
	item.IACLs = it.IACLs

	return item.IACLs, err
}

func (item *RodsItem) AddACLs(acls []ACL) error {
	it := CopyRodsItem(*item)
	it.IACLs = acls
	if _, err := item.client.Chmod(Args{}, it); err != nil {
		return err
	}

	_, err := item.FetchACLs()
	return err
}

// Metadata returns the RodsItem AVUs. It does not fetch AVUs from the server.
// See FetchMetadata().
func (item *RodsItem) Metadata() []AVU {
	return item.IAVUs
}

// HasMetadatum returns true if the RodsItem has the argument AVU in its
// metadata.
func (item *RodsItem) HasMetadatum(avu AVU) bool {
	for _, a := range item.IAVUs {
		if a == avu {
			return true
		}
	}
	return false
}

// HasSomeMetadata returns true if the RodsItem has at least one of the argument
// AVUs in its metadata.
func (item *RodsItem) HasSomeMetadata(avus []AVU) bool {
	lookup := make(map[AVU]bool)
	for _, avu := range item.IAVUs {
		lookup[avu] = true
	}

	for _, avu := range avus {
		if lookup[avu] {
			return true
		}
	}

	return false
}

// HasAllMetadata returns true if the RodsItem has at has every one of the
// argument AVUs in its metadata.
func (item *RodsItem) HasAllMetadata(avus []AVU) bool {
	lookup := make(map[AVU]bool)
	for _, avu := range item.IAVUs {
		lookup[avu] = true
	}

	for _, avu := range avus {
		if !lookup[avu] {
			return false
		}
	}

	return true
}

// FetchMetadata fetches and returns any metadata AVUs on the RodsItem.
func (item *RodsItem) FetchMetadata() ([]AVU, error) {
	it, err := item.client.ListItem(Args{AVU: true}, *item)
	if err != nil {
		return []AVU{}, err
	}
	item.IAVUs = it.IAVUs

	return item.IAVUs, err
}

// AddMetadata adds each argument AVU to the RodsItem, in the order that they
// are supplied. The add operation is idempotent (adding an AVU that is already
// present does return an error).
func (item *RodsItem) AddMetadata(avus []AVU) error {
	currentAVUs, err := item.FetchMetadata()
	if err != nil {
		return err
	}

	toAdd := SetDiffAVUs(avus, currentAVUs)
	if len(toAdd) > 0 {
		it := CopyRodsItem(*item)
		it.IAVUs = toAdd
		if _, err := item.client.MetaAdd(Args{}, it); err != nil {
			return err
		}

		item.IAVUs = SetUnionAVUs(currentAVUs, toAdd)
	}

	return err
}

// RemoveMetadata removes each argument AVU from the RodsItem, in the order
// that they are supplied. The remove operation is idempotent (removing an AVU
// that is not present does not return an error).
func (item *RodsItem) RemoveMetadata(avus []AVU) error {
	currentAVUs, err := item.FetchMetadata()
	if err != nil {
		return err
	}

	it := CopyRodsItem(*item)
	it.IAVUs = avus
	if _, err := item.client.MetaRem(Args{}, it); err != nil {
		return err
	}

	item.IAVUs = SetDiffAVUs(currentAVUs, avus)

	return err
}

// ReplaceMetadata removes from a RodsItem any existing AVUs sharing an
// attribute with the argument AVUs and then adds to the RodsItem the argument
// AVUs.
func (item *RodsItem) ReplaceMetadata(avus []AVU) error {

	// Attributes whose AVUs are to be replaced
	repAttrs := make(map[string]struct{})
	for _, avu := range avus {
		repAttrs[avu.Attr] = struct{}{}
	}

	currentAVUs, err := item.FetchMetadata()
	if err != nil {
		return err
	}

	// These are in the both the existing and replacement sets. Avoid removing
	// them.
	toKeep := SetIntersectAVUs(avus, currentAVUs)

	var toRemove []AVU
	for _, avu := range currentAVUs {
		if _, ok := repAttrs[avu.Attr]; ok {
			if !SearchAVU(avu, toKeep) {
				toRemove = append(toRemove, avu)
			}
		}
	}

	toAdd := SetDiffAVUs(avus, toKeep)

	rem := CopyRodsItem(*item)
	rem.IAVUs = toRemove

	log := logs.GetLogger()
	log.Debug().Str("path", item.String()).
		Str("operation", "remove_avu").Msgf("%v", toRemove)

	if len(toRemove) > 0 {
		if _, err := item.client.MetaRem(Args{}, rem); err != nil {
			return err
		}
	}

	log.Debug().Str("path", item.String()).
		Str("operation", "keep_avu").Msgf("%v", toKeep)

	log.Debug().Str("path", item.String()).
		Str("operation", "add_avu").Msgf("%v", toAdd)

	if len(toAdd) > 0 {
		add := CopyRodsItem(*item)
		add.IAVUs = toAdd

		if _, err := item.client.MetaAdd(Args{}, add); err != nil {
			return err
		}
	}

	// Update our cache of the final AVUs. We could call item.FetchMetadata()
	// again, but this saves a trip to the server.
	var final = SetUnionAVUs(toAdd, SetDiffAVUs(currentAVUs, toRemove))
	SortAVUs(final)

	log.Debug().Str("path", item.String()).Msgf("AVUs %v", final)

	item.IAVUs = final

	return err
}

func CopyRodsItem(item RodsItem) RodsItem {
	return RodsItem{
		client:      item.client,
		IFile:       item.IFile,
		IDirectory:  item.IDirectory,
		IPath:       item.IPath,
		IName:       item.IName,
		IChecksum:   item.IChecksum,
		ISize:       item.ISize,
		IACLs:       item.IACLs,
		IAVUs:       item.IAVUs,
		IContents:   item.IContents,
		IReplicates: item.IReplicates,
		ITimestamps: item.ITimestamps,
	}
}

// SortRodsItems sorts items by Path, sorting Collections before DataObjects.
func SortRodsItems(items []RodsItem) {
	sort.SliceStable(items, func(i, j int) bool {

		iColl := items[i].IsCollection()
		jColl := items[j].IsCollection()

		switch {
		case iColl && !jColl: // collection / data object
			return true
		case !iColl && jColl: // data object / collection
			return false
		case iColl && jColl: // collection / collection
			return items[i].IPath < items[j].IPath
		case items[i].IPath == items[j].IPath: // data object / data object
			return items[i].IName < items[j].IName
		}

		// data object / data object
		return items[i].IPath < items[j].IPath
	})
}

// ACL is an access control list. Owner may be a user, or more often, a data
// access group.
type ACL struct {
	// Owner is the iRODS group (or user)
	Owner string `json:"owner"`
	// Level is the iRODS access level (null, read, own etc)
	Level string `json:"level"`
	// Zone is the iRODS zone
	Zone string `json:"zone"`
}

// SortACLs sorts acls by Zone, then Owner and finally, Level.
func SortACLs(acls []ACL) {
	sort.SliceStable(acls, func(i, j int) bool {
		return acls[i].Zone < acls[j].Zone ||
			acls[i].Owner < acls[j].Owner ||
			acls[i].Level < acls[j].Level
	})
}

// AVU is an iRODS attribute, value, units triple.
type AVU struct {
	Attr     string `json:"attribute"`       // iRODS attribute name
	Value    string `json:"value"`           // iRODS attribute value
	Units    string `json:"units,omitempty"` // iRODS attribute units
	Operator string `json:"operator,omitempty"`
}

// HasNamespace returns true if the AVU attribute has a colon-separated
// namespace.
func (avu AVU) HasNamespace() bool {
	return strings.Index(avu.Attr, ":") > 0
}

// Namespace returns the namespace string of the AVU attribute, or an
// empty string if there is no namespace.
func (avu AVU) Namespace() string {
	if avu.HasNamespace() {
		return strings.Split(avu.Attr, ":")[0]
	}
	return ""
}

// WithNamespace returns an AVU with the attribute namespace set.
func (avu AVU) WithNamespace(namespace string) AVU {
	nsPrefix := namespace + ":"

	nsAvu := avu
	if !strings.HasPrefix(nsAvu.Attr, nsPrefix) {
		nsAvu.Attr = nsPrefix + nsAvu.Attr
	}
	return nsAvu
}

// WithoutNamespace returns the attribute of the AVU without any namespace,
// if one is present. If there is no namespace, the AVU attribute is
// returned.
func (avu AVU) WithoutNamespace() string {
	if avu.HasNamespace() {
		return strings.Split(avu.Attr, ":")[1]
	}

	return avu.Attr
}

// SortAVUs sorts avus by Attr, then Value and finally, Units.
func SortAVUs(avus []AVU) {
	sort.SliceStable(avus, func(i, j int) bool {
		return avus[i].Attr < avus[j].Attr ||
			avus[i].Value < avus[j].Value ||
			avus[i].Units < avus[j].Units
	})
}

// Replicate is a data object replicate.
type Replicate struct {
	// Resource is the resource name where the replicate is located
	Resource string `json:"resource"`
	// Location is the server where the replicate is located
	Location string `json:"location"`
	// Checksum is the checksum of the replicate
	Checksum string `json:"checksum"`
	// Number is iRODS' replicate number
	Number uint16 `json:"number"`
	// Valid is iRODS' flag describing whether the replicate is up-to-date
	Valid bool `json:"valid"`
}

// SortReplicates sorts reps by Resource, then Location, then Number, then
// Checksum and finally, Valid.
func SortReplicates(reps []Replicate) {
	sort.SliceStable(reps, func(i, j int) bool {
		return reps[i].Resource < reps[j].Resource ||
			reps[i].Location < reps[j].Location ||
			reps[i].Number < reps[j].Number ||
			reps[i].Checksum < reps[j].Checksum ||
			(reps[i].Valid && !reps[j].Valid)
	})
}

type Timestamp struct {
	// Created time of the replicate
	Created  time.Time `json:"created,omitempty"`
	// Modified time of the replicate
	Modified time.Time `json:"modified,omitempty"`
	// Replicates is the replicate number the timestamp refers to
	Replicates int `json:"replicates,omitempty"`
}

// SortTimestamps sorts times by Replicates, Created and then by Modified.
func SortTimestamps(times []Timestamp) {
	// Less if Replicates i < Replicates j.
	//
	// Less if the Created time is non-Zero and the other Created time is Zero
	// or is later.
	//
	// Less if the Modified time is non-Zero time and other Modified time is
	// Zero or is later.
	sort.SliceStable(times, func(i, j int) bool {
		if times[i].Replicates < times[j].Replicates {
			return true
		}
		if times[i].Replicates > times[j].Replicates {
			return false
		}

		ci := !times[i].Created.IsZero() // Is times creation time
		cj := !times[j].Created.IsZero()
		mi := !times[i].Modified.IsZero() // Is times modification time
		mj := !times[j].Modified.IsZero()

		return (ci && !cj) || (!mi && mj) ||
			(ci && cj && times[i].Created.Before(times[j].Created)) ||
			(mi && mj && times[i].Modified.Before(times[j].Modified))
	})
}
