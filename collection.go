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

import "path/filepath"

type Collection struct {
	*RemoteItem
}

func NewCollection(client *Client, remotePath string) *Collection {
	remotePath = filepath.Clean(remotePath)

	return &Collection{RemoteItem: &RemoteItem{
		client, &RodsItem{IPath: remotePath}}}
}

func MakeCollection(client *Client, remotePath string) (*Collection, error) {
	remotePath = filepath.Clean(remotePath)

	item, err := client.MkDir(Args{Recurse: true}, RodsItem{IPath: remotePath})
	if err != nil {
		return nil, err
	}

	item, err = client.ListItem(Args{}, item)
	if err != nil {
		return nil, err
	}

	coll := &Collection{RemoteItem: &RemoteItem{client, &item}}

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

	coll := &Collection{RemoteItem: &RemoteItem{client, &item}}

	return coll, err
}

func (coll *Collection) Ensure() error {
	exists, err := coll.Exists()
	if err != nil {
		return err
	}
	if !exists {
		if _, err:= MakeCollection(coll.client, coll.RodsPath()); err != nil {
			return err
		}
	}

	return nil
}

func (coll *Collection) Remove() error {
	_, err := coll.RemoteItem.client.
		RemDir(Args{}, *coll.RemoteItem.RodsItem)
	return err
}

func (coll *Collection) RemoveRecurse() error {
	_, err := coll.RemoteItem.client.
		RemDir(Args{Recurse: true}, *coll.RemoteItem.RodsItem)
	return err
}

func (coll *Collection) Exists() (bool, error) {
	return coll.RemoteItem.Exists()
}

func (coll *Collection) LocalPath() string {
	return coll.RemoteItem.LocalPath()
}

func (coll *Collection) RodsPath() string {
	return coll.RemoteItem.RodsPath()
}

func (coll *Collection) String() string {
	return coll.RemoteItem.String()
}

func (coll *Collection) ACLs() []ACL {
	return coll.IACLs
}

func (coll *Collection) FetchACLs() ([]ACL, error) {
	return coll.RemoteItem.FetchACLs()
}

func (coll *Collection) AddACLs(acls []ACL) error {
	return coll.RemoteItem.AddACLs(acls)
}

func (coll *Collection) Metadata() []AVU {
	return coll.IAVUs
}

func (coll *Collection) FetchMetadata() ([]AVU, error) {
	return coll.RemoteItem.FetchMetadata()
}

func (coll *Collection) AddMetadata(avus []AVU) error {
	return coll.RemoteItem.AddMetadata(avus)
}

func (coll *Collection) RemoveMetadata(avus []AVU) error {
	return coll.RemoteItem.RemoveMetadata(avus)
}

func (coll *Collection) ReplaceMetadata(avus []AVU) error {
	return coll.RemoteItem.ReplaceMetadata(avus)
}
