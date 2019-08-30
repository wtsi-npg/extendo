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
 * @file metadata.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package extendo

import (
	"fmt"
	"os/user"
	"sort"
	"time"

	dcterms "extendo/dublincore"
)

func MakeAVU(attr string, value string, units ...string) AVU {
	var unit string
	if len(units) > 0 {
		unit = units[0]
	}

	return AVU{Attr: attr, Value: value, Units: unit}
}

func MakeCreationMetadata() []AVU {
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
	}
}

// SearchAVU returns true if avu is found in the slice of AVUs. The function
// sorts slice of AVUs if it is unsorted.
func SearchAVU(avu AVU, avus []AVU) bool {
	arr := AVUArr(avus)
	if !sort.IsSorted(arr) {
		sort.Sort(arr)
	}
	i := sort.Search(len(arr), func(i int) bool {
		return !AVULess(arr[i], avu)
	})

	if i < len(arr) && avus[i] == avu {
		return true
	}
	return false
}

// SetIntersectAVUs returns a sorted slice of AVUs containing the intersection
// of the two slice arguments.
func SetIntersectAVUs(x []AVU, y []AVU) []AVU {
	mx := make(map[AVU]struct{})

	for _, avu := range x {
		mx[avu] = struct{}{}
	}

	var intersection AVUArr
	for _, avu := range y {
		if _, ok := mx[avu]; ok {
			intersection = append(intersection, avu)
		}
	}

	sort.Sort(intersection)
	return intersection
}

// SetUnionAVUs returns a sorted slice of AVUs containing the union
// of the two slice arguments.
func SetUnionAVUs(x []AVU, y []AVU) []AVU {
	mx := make(map[AVU]struct{})

	var union AVUArr
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

	sort.Sort(union)
	return union
}

// SetDiffAVUs returns a sorted slice of AVUs containing the set difference
// between the x and y slice arguments.
func SetDiffAVUs(x []AVU, y []AVU) []AVU {
	my := make(map[AVU]struct{})

	for _, avu := range y {
		my[avu] = struct{}{}
	}

	var diff AVUArr
	for _, avu := range x {
		if _, ok := my[avu]; !ok {
			diff = append(diff, avu)
		}
	}

	sort.Sort(diff)
	return diff
}

// UniqAVUs returns a newly allocated, sorted slice of AVUs containing no
// duplicates.
func UniqAVUs(avus []AVU) []AVU {
	uniq := make([]AVU, 0, len(avus))

	for i, avu := range SortAVUs(avus) {
		if i == 0 {
			uniq = append(uniq, avu)
		} else {
			if avu != uniq[len(uniq)-1] {
				uniq = append(uniq, avu)
			}
		}
	}

	return uniq
}

func SortAVUs(avus AVUArr) []AVU {
	sort.Sort(avus)
	return avus
}
