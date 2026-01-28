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

func (m alias_collect) apply_raw(field string){
	start := 0
	for {
		//	Get next "."
		pos := strings.IndexByte(field[start:], '.')
		if pos == -1 {
			break
		}
		abs_pos		:= start + pos
		word_start	:= abs_pos
		//	Move backwards to find the whole alias
		for word_start > 0 {
			char := field[word_start-1]
			//	Valid char in alias/table name
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '<' || char == '>') {
				break
			}
			word_start--
		}
		if word_start < abs_pos {
			alias := field[word_start:abs_pos]
			if alias != ROOT_ALIAS {
				m[alias] = struct{}{}
			}
		}
		//	Move cursor to after the "." just found
		start = abs_pos + 1
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