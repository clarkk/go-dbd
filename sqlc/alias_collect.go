package sqlc

import (
	"maps"
	"slices"
	"strings"
)

type alias_collect map[string]struct{}

func (m alias_collect) apply(field string){
	if pos := strings.IndexByte(field, '.'); pos != -1 {
		m[field[:pos]] = struct{}{}
	}
}

func (m alias_collect) merge(a alias_collect){
	maps.Copy(m, a)
}

func (m alias_collect) sorted() []string {
	keys := make([]string, len(m))
	var i int
	for k := range m {
		keys[i] = k
		i++
	}
	slices.Sort(keys)
	return keys
}