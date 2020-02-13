/*
 * Copyright (C) 2019, 2020. Genome Research Ltd. All rights reserved.
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
 * @file metadata.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"fmt"
	"os/user"
	"time"

	dcterms "github.com/kjsanger/extendo/v2/dublincore"
)

const ChecksumAttr string = "md5"

type AVUFilter func(avu AVU) bool

func MakeAVU(attr string, value string, units ...string) AVU {
	var unit string
	if len(units) > 0 {
		unit = units[0]
	}

	return AVU{Attr: attr, Value: value, Units: unit}
}

func MakeCreationMetadata(checksum string) []AVU {
	when := time.Now().Format(time.RFC3339)
	who, err := user.Current()
	if err != nil {
		panic("failed to lookup the current system user")
	}

	publisher := fmt.Sprintf("ldap://ldap.internal.sanger.ac.uk/"+
		"ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid=%s)", who.Name)

	return []AVU{
		MakeAVU(dcterms.Created, when),
		MakeAVU(dcterms.Creator, "http://www.sanger.ac.uk"),
		MakeAVU(dcterms.Publisher, publisher),
		MakeAVU(ChecksumAttr, checksum),
	}
}

// SearchAVU returns true if avu is found in the slice of AVUs.
func SearchAVU(avu AVU, avus []AVU) bool {
	m := make(map[AVU]struct{})

	for _, avu := range avus {
		m[avu] = struct{}{}
	}

	_, ok := m[avu]
	return ok
}

func FilterAVUs(avus []AVU, f AVUFilter) []AVU {
	var match []AVU
	for _, avu := range avus {
		if f(avu) {
			match = append(match, avu)
		}
	}

	return match
}

// SetIntersectAVUs returns a sorted slice of AVUs containing the intersection
// of the two slice arguments.
func SetIntersectAVUs(x []AVU, y []AVU) []AVU {
	mx := make(map[AVU]struct{})

	for _, avu := range x {
		mx[avu] = struct{}{}
	}

	var intersection []AVU
	for _, avu := range y {
		if _, ok := mx[avu]; ok {
			intersection = append(intersection, avu)
		}
	}

	SortAVUs(intersection)
	return intersection
}

// SetUnionAVUs returns a sorted slice of AVUs containing the union
// of the two slice arguments.
func SetUnionAVUs(x []AVU, y []AVU) []AVU {
	mx := make(map[AVU]struct{})

	var union []AVU
	for _, avu := range x {
		if _, ok := mx[avu]; !ok {
			mx[avu] = struct{}{}
			union = append(union, avu)
		}
	}

	for _, avu := range y {
		if _, ok := mx[avu]; !ok {
			union = append(union, avu)
		}
	}

	SortAVUs(union)
	return union
}

// SetDiffAVUs returns a sorted slice of AVUs containing the set difference
// between the x and y slice arguments.
func SetDiffAVUs(x []AVU, y []AVU) []AVU {
	my := make(map[AVU]struct{})

	for _, avu := range y {
		my[avu] = struct{}{}
	}

	var diff []AVU
	for _, avu := range x {
		if _, ok := my[avu]; !ok {
			diff = append(diff, avu)
		}
	}

	SortAVUs(diff)
	return diff
}

// UniqAVUs returns a newly allocated, sorted slice of AVUs containing no
// duplicates.
func UniqAVUs(avus []AVU) []AVU {
	m := make(map[AVU]struct{})
	for _, avu := range avus {
		m[avu] = struct{}{}
	}

	var uniq []AVU
	for avu := range m {
		uniq = append(uniq, avu)
	}

	SortAVUs(uniq)
	return uniq
}
