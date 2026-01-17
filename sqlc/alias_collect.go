package sqlc

import (
	"sync"
	"slices"
	"strings"
)

var alias_collect_pool = sync.Pool{
	New: func() any {
		//	Pre-allocation
		return make(alias_collect, 4)
	},
}

type alias_collect map[string]struct{}

func (m alias_collect) apply(field string){
	if pos := strings.IndexByte(field, '.'); pos != -1 {
		alias := field[:pos]
		if alias == ROOT_ALIAS {
			return
		}
		m[alias] = struct{}{}
	}
}

func (m alias_collect) filter(joins []join) []join {
	filtered := make([]join, 0, len(joins)) 
	for _, j := range joins {
		if _, ok := m[j.t]; ok {
			filtered = append(filtered, j)
		}
	}
	return filtered
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

func (m alias_collect) reset(){
	clear(m)
}