package sqlc

import "slices"

const (
	alloc_field			= 15
	alloc_join_clause	= 50
	alloc_query			= 200
)

type (
	SQL interface {
		Compile() (string, []any, error)
	}
	
	Map map[string]any
	
	query struct {
		table	string	//	Base table
	}
)

func fields_from_map(fields map[string]any) *Fields_clause {
	//	Sort keys
	keys := make([]string, len(fields))
	var i int
	for field := range fields {
		keys[i] = field
		i++
	}
	slices.Sort(keys)
	
	clause := Fields()
	for _, field := range keys {
		clause.Value(field, fields[field])
	}
	return clause
}

func (q *query_join) alloc_field_list(count int, use_alias bool) int {
	alloc := alloc_field + 2	//	", "
	if use_alias {
		alloc += 1 + len(q.t)
	}
	return alloc * count
}

func alloc_field_assign(count int) int {
	return count * (alloc_field + 4)	//	"=?, "
}

func alloc_field_placeholder_list(count int) int {
	return max(0, (count * 2) - 1)		//	?,?,?
}