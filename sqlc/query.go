package sqlc

import (
	"fmt"
	"sync"
	"strings"
)

const (
	alloc_field			= 15
	alloc_join_clause	= 50
	alloc_query			= 200
)

var compiler_pool = sync.Pool{
	New: func() any {
		//	Pre-allocation
		return &compiler{
			tables:	make(map[string]string, 5),
			data:	make([]any, 0, 5),
		}
	},
}

type (
	SQL interface {
		Compile() (string, []any, error)
	}
	
	Map map[string]any
	
	query struct {
		table	string
	}
)

func SQL_debug(q SQL) string {
	sql, data, err := q.Compile()
	if err != nil {
		return "Error compiling SQL: "+err.Error()
	}
	for _, value := range data {
		sql = strings.Replace(sql, "?", fmt.Sprintf("%v", value), 1)
	}
	return strings.TrimSpace(sql)
}

func SQL_error(msg string, q SQL, err error) string {
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func field_placeholder_list(count int, sb *sbuilder){
	if count == 0 {
		return
	}
	sb.WriteByte('?')
	for i := 1; i < count; i++ {
		sb.WriteString(",?")
	}
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