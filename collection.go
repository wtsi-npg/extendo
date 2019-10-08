/*
 * Copyright (C) 2019. Genome Research Ltd. All rights reserved.
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
 * @file collection.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"path/filepath"
	"time"

	logs "github.com/kjsanger/logshim"
	"github.com/pkg/errors"
)

type Collection struct {
	*RodsItem
}

func NewCollection(client *Client, remotePath string) *Collection {
	remotePath = filepath.Clean(remotePath)

	return &Collection{&RodsItem{client: client, IPath: remotePath}}
}

func MakeCollection(client *Client, remotePath string) (*Collection, error) {
	remotePath = filepath.Clean(remotePath)

	item, err := client.MkDir(Args{Recurse: true}, RodsItem{IPath: remotePath})
	if err != nil {
		return nil, err
	}

	item.client = client
	coll := &Collection{&item}

	// iRODS should not return from MakeCollection until the collection is
	// made. However, I have observed that the iRODS 4.1.12 server will do so,
	// leading to the failure of any operations the client performs on the
	// returned collection.
	//
	// This retry is a workaround to block and wait for the collection to
	// appear. It's quite ugly, but simple and fixes

	log := logs.GetLogger()

	var exists bool
	maxTries, backoffFactor := 3, 2

	for try := 0; try < maxTries; try++ {
		exists, err = coll.Exists()
		if exists || err != nil {
			break
		}

		delay := try * backoffFactor
		log.Debug().Str("path", remotePath).
			Int("try", try).Int("seconds", delay).
			Msg("waiting for collection to appear")

		time.Sleep(time.Second * time.Duration(delay))
	}

	if !exists {
		err = errors.Errorf("timed out waiting for "+
			"collection '%s' to appear", remotePath)
	}

	return coll, err
}

func PutCollection(client *Client, localPath string, remotePath string,
	avus ...[]AVU) (*Collection, error) {

	localPath = filepath.Clean(localPath)
	remotePath = filepath.Clean(remotePath)

	item := RodsItem{IDirectory: localPath, IPath: remotePath}
	putArgs := Args{Force: true, Recurse: true}

	if len(avus) > 0 {
		putArgs.AVU = true
		var x []AVU
		for _, y := range avus {
			x = append(x, y...)
		}
		item.IAVUs = x
	}

	if _, err := client.Put(putArgs, item); err != nil {
		return nil, err
	}

	item, err := client.ListItem(Args{}, item)
	if err != nil {
		return nil, err
	}

	item.client = client
	coll := &Collection{ &item}

	return coll, err
}

func (coll *Collection) Ensure() error {
	exists, err := coll.Exists()
	if err != nil {
		return err
	}
	if !exists {
		if _, err := MakeCollection(coll.client, coll.RodsPath()); err != nil {
			return err
		}
	}

	return nil
}

func (coll *Collection) Remove() error {
	_, err := coll.client.RemDir(Args{}, *coll.RodsItem)
	return err
}

func (coll *Collection) RemoveRecurse() error {
	_, err := coll.client.RemDir(Args{Recurse: true}, *coll.RodsItem)
	return err
}

// Contents returns the Collections from the collection contents. If the
// contents have not been Fetched, the slice will be empty.
func (coll *Collection) Collections() []Collection {
	var colls []Collection

	for i := range coll.IContents {
		if coll.IContents[i].IsCollection() {
			colls = append(colls, Collection{&coll.IContents[i]})
		}
	}

	return colls
}

// Contents returns the DataObjects from the collection contents. If the
// contents have not been Fetched, the slice will be empty.
func (coll *Collection) DataObjects() []DataObject {
	var objs []DataObject

	for i := range coll.IContents {
		if coll.IContents[i].IsDataObject() {
			objs = append(objs, DataObject{&coll.IContents[i]})
		}
	}

	return objs
}

// Contents returns the collection contents. If the contents have not been
// Fetched, the slice will be empty.
func (coll *Collection) Contents() []RodsItem {
	return coll.IContents
}

// FetchContents returns a shallow list of the item contents, freshly
// fetched from the server. It caches the slice for future calls to Contents.
func (coll *Collection) FetchContents() ([]RodsItem, error) {
	it, err := coll.client.ListItem(Args{Contents: true, Recurse: false}, *coll.RodsItem)
	if err != nil {
		return []RodsItem{}, err
	}
	coll.IContents = it.IContents

	return coll.IContents, err
}

// FetchContentsRecurse returns a recursive list of the item contents,
// freshly fetched from the server. It caches the slice for future calls to
// Contents.
func (coll *Collection) FetchContentsRecurse() ([]RodsItem, error) {
	items, err := coll.client.List(Args{Contents: true, Recurse: true}, *coll.RodsItem)
	if err != nil {
		return []RodsItem{}, err
	}
	coll.IContents = items

	return coll.IContents, err
}
