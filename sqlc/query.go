package sqlc

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