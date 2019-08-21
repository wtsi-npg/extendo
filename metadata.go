package extendo

import (
	"fmt"
	"os/user"
	"sort"
	"time"

	dcterms "extendo/dublincore"
	"github.com/kjsanger/logshim"
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
		panic("failed to lookup current who")
	}

	publisher := fmt.Sprintf("ldap://ldap.internal.sanger.ac.uk/"+
		"ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid=%s)", who.Name)

	return []AVU{
		MakeAVU(dcterms.Created, when),
		MakeAVU(dcterms.Creator, "http://www.sanger.ac.uk"),
		MakeAVU(dcterms.Publisher, publisher),
	}
}

// SearchAVU returns true if avu is found in avus. The function sorts avus if
// necessary.
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
	} else {
		return false
	}
}

// IntersectionAVUs returns a sorted slice of AVUs contaning the intersection of
// the two slice arguments.
func IntersectionAVUs(x []AVU, y []AVU) []AVU {
	var shortest, longest AVUArr
	if len(x) < len(y) {
		shortest, longest = x, y
	} else {
		shortest, longest = y, x
	}

	// Search shortest because SearchAVU sorts its array, currently
	var intersection AVUArr
	for _, avu := range longest {
		if SearchAVU(avu, shortest) {
			intersection = append(intersection, avu)
		}
	}

	sort.Sort(intersection)
	return intersection
}

// UniqAVUs returns a sorted slice of AVUs with any duplicates removed.
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

	return SortAVUs(uniq)
}

func SortAVUs(avus AVUArr) []AVU {
	sort.Sort(avus)
	return avus
}

// ReplaceAVUs removes any existing AVUs sharing an attribute with the argument
// AVUs and then adds the argument AVUs,
func (client *Client) ReplaceAVUs(item RodsItem, avus []AVU) (RodsItem, error) {
	remAttrs := make(map[string]struct{})
	for _, avu := range avus {
		remAttrs[avu.Attr] = struct{}{}
	}

	// These are in the existing and replacement sets. Avoid removing them.
	keepAVUs := IntersectionAVUs(avus, item.IAVUs)

	var remAVUs []AVU
	for _, avu := range item.IAVUs {
		if _, ok := remAttrs[avu.Attr]; ok {
			if !SearchAVU(avu, keepAVUs) {
				remAVUs = append(remAVUs, avu)
			}
		}
	}

	var addAVUS []AVU
	for _, avu := range avus {
		if !SearchAVU(avu, keepAVUs) {
			addAVUS = append(addAVUS, avu)
		}
	}

	log := logshim.GetLogger()
	rem := CopyRodsItem(item)
	rem.IAVUs = remAVUs

	if len(remAVUs) > 0 {
		log.Info().Str("path", item.String()).
			Str("operation", "remove").Msgf("%+v", remAVUs)
		_, err := client.MetaRem(Args{}, rem)
		if err != nil {
			return item, err
		}
	}

	if len(keepAVUs) > 0 {
		log.Info().Str("path", item.String()).
			Str("operation", "none").Msgf("%+v", keepAVUs)
	}

	if len(addAVUS) > 0 {
		add := CopyRodsItem(item)
		add.IAVUs = addAVUS
		log.Info().Str("path", item.String()).
			Str("operation", "add").Msgf("%+v", addAVUS)
		_, err := client.MetaAdd(Args{}, add)
		if err != nil {
			return item, err
		}
	}

	return item, nil
}

func uniq(items []string) []string {
	set := make(map[string]struct{})

	for _, item := range items {
		set[item] = struct{}{}
	}

	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}
