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

var builder_pool = sync.Pool{
	New: func() any {
		return &sbuilder{}
	},
}

type (
	SQL interface {
		Compile() (string, error)
		Data() []any
	}
	
	Map map[string]any
	
	query struct {
		table 	string
		data 	[]any
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string
		join_t			string
		field 			string
		field_foreign 	string
		conditions		Map
	}
)

func SQL_debug(q SQL) string {
	s, _ := q.Compile()
	for _, value := range q.Data() {
		s = strings.Replace(s, "?", fmt.Sprintf("%v", value), 1)
	}
	return strings.TrimSpace(s)
}

func SQL_error(msg string, q SQL, err error) string {
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func (q *query) Data() []any {
	return q.data
}

func (q *query) append_data(val any){
	if val == nil {
		return
	}
	
	//	Flatten data slices
	if v, ok := val.([]any); ok {
		length := len(v)
		if length == 0 {
			return
		}
		
		q.alloc_data_capacity(len(q.data) + length)
		
		q.data = append(q.data, v...)
	} else {
		q.data = append(q.data, val)
	}
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

func (q *query) alloc_data_capacity(total int){
	if cap(q.data) < total {
		new_data := make([]any, len(q.data), total)
		copy(new_data, q.data)
		q.data = new_data
	}
}

func (q *query_join) alloc_field_list(count int) int {
	alloc := alloc_field + 2	//	", "
	if q.use_alias {
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