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
		Compile() (string, error)
		Data() []any
	}
	
	Map map[string]any
	
	query struct {
		table 			string
		data_compiled	[]any
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string	//	Table alias
		join_t			string	//	Join on a non-base (pre-defined) table (table alias)
		field 			string
		field_foreign 	string
		conditions		Map
		depth			int
	}
)

func SQL_debug(q SQL) string {
	sql, _ := q.Compile()
	for _, value := range q.Data() {
		sql = strings.Replace(sql, "?", fmt.Sprintf("%v", value), 1)
	}
	return strings.TrimSpace(sql)
}

func SQL_error(msg string, q SQL, err error) string {
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func (q *query) Data() []any {
	return q.data_compiled
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