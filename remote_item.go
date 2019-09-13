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
 * @file remote_item.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"sort"

	logs "github.com/kjsanger/logshim"
	"github.com/pkg/errors"
)

type RemoteItem struct {
	client *Client
	*RodsItem
}

func (path RemoteItem) Exists() (bool, error) {
	_, err := path.client.ListItem(Args{}, *path.RodsItem)
	if err != nil {
		switch err := errors.Cause(err).(type) {
		case *RodsError:
			if err.Code() == RodsUserFileDoesNotExist {
				return false, nil
			}
			return false, err

		default:
			return false, err
		}
	}

	return true, nil
}

func (path RemoteItem) LocalPath() string {
	return path.RodsItem.LocalPath()
}

func (path RemoteItem) RodsPath() string {
	return path.RodsItem.RodsPath()
}

func (path RemoteItem) String() string {
	return path.RodsItem.String()
}

func (path RemoteItem) FetchACLs() ([]ACL, error) {
	item, err := path.client.ListItem(Args{ACL: true}, *path.RodsItem)
	if err != nil {
		return []ACL{}, err
	}
	path.IACLs = item.IACLs

	return path.IACLs, err
}

func (path RemoteItem) AddACLs(acls []ACL) error {
	item := CopyRodsItem(*path.RodsItem)
	item.IACLs = acls
	if _, err := path.client.Chmod(Args{}, item); err != nil {
		return err
	}

	_, err := path.FetchACLs()
	return err
}

func (path RemoteItem) FetchMetadata() ([]AVU, error) {
	item, err := path.client.ListItem(Args{AVU: true}, *path.RodsItem)
	if err != nil {
		return []AVU{}, err
	}
	path.IAVUs = item.IAVUs

	return path.IAVUs, err
}

func (path RemoteItem) AddMetadata(avus []AVU) error {
	item := CopyRodsItem(*path.RodsItem)
	item.IAVUs = avus

	if _, err := path.client.MetaAdd(Args{}, item); err != nil {
		return err
	}

	_, err := path.FetchMetadata()
	return err
}

func (path RemoteItem) RemoveMetadata(avus []AVU) error {
	item := CopyRodsItem(*path.RodsItem)
	item.IAVUs = avus
	if _, err := path.client.MetaRem(Args{}, item); err != nil {
		return err
	}

	_, err := path.FetchMetadata()
	return err
}

// ReplaceMetadata removes from a RodsItem any existing AVUs sharing an attribute
// with the argument AVUs and then adds to the RodsItem the argument AVUs.
func (path RemoteItem) ReplaceMetadata(avus []AVU) error {

	// Attributes whose AVUs are to be replaced
	repAttrs := make(map[string]struct{})
	for _, avu := range avus {
		repAttrs[avu.Attr] = struct{}{}
	}

	currentAVUs, err := path.FetchMetadata()
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

	rem := CopyRodsItem(*path.RodsItem)
	rem.IAVUs = toRemove

	log := logs.GetLogger()
	log.Debug().Str("path", path.String()).
		Str("operation", "remove_avu").Msgf("%v", toRemove)

	if len(toRemove) > 0 {
		if _, err := path.client.MetaRem(Args{}, rem); err != nil {
			return err
		}
	}

	log.Debug().Str("path", path.String()).
		Str("operation", "keep_avu").Msgf("%v", toKeep)

	log.Debug().Str("path", path.String()).
		Str("operation", "add_avu").Msgf("%v", toAdd)

	if len(toAdd) > 0 {
		item := CopyRodsItem(*path.RodsItem)
		item.IAVUs = toAdd

		if _, err := path.client.MetaAdd(Args{}, item); err != nil {
			return err
		}
	}

	// Update our cache of the final AVUs. We could call path.FetchMetadata()
	// again, but this saves a trip to the server.
	var final AVUArr = SetUnionAVUs(toAdd, SetDiffAVUs(currentAVUs, toRemove))
	sort.Sort(final)

	log.Debug().Str("path", path.String()).Msgf("AVUs %v", final)

	path.IAVUs = final

	return err
}
