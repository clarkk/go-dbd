package sqlc

import (
	"fmt"
	"strings"
)

type (
	SQL interface {
		Compile() (string, error)
		Data() []any
	}
	
	Map map[string]any
	
	query struct {
		table 	string
		t 		string
		joins 	[]join
		tables 	map[string]string
		joined 	bool
		data 	[]any
	}
	
	query_where struct {
		query
		where 		[]where_clause
		where_data 	[]any
		id 			int
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string
		field 			string
		field_foreign 	string
	}
	
	where_clause struct {
		field 		string
		sql 		string
	}
)

func SQL_debug(q SQL) string {
	s, _ := q.Compile()
	for _, value := range q.Data() {
		s = strings.Replace(s, "?", fmt.Sprintf("%v", value), 1)
	}
	return strings.TrimSpace(s)
}

func (q *query) Data() []any {
	return q.data
}

func (q *query) left_join(table, t, field, field_foreign string){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			"LEFT JOIN",
		table:			table,
		t:				t,
		field:			field,
		field_foreign:	field_foreign,
	})
}

func (q *query) compile_tables() error {
	t := string(q.table[0])
	q.tables = map[string]string{}
	if q.joined {
		//	Check for char collisions in joined tables
		for _, j := range q.joins {
			if _, ok := q.tables[j.t]; ok {
				return fmt.Errorf("Join table short already used: %s (%s)", j.t, j.table)
			}
			q.tables[j.t] = j.table
		}
		//	Get available char for base table (a-z)
		if _, ok := q.tables[t]; ok {
			const ascii_a = 97
			for i := 0; i < 26; i++ {
				t = string(rune(ascii_a+i))
				if _, ok := q.tables[t]; !ok {
					q.tables[t] = q.table
					break
				}
			}
		}
	}
	q.t = t
	return nil
}

func (q *query) compile_joins() string {
	var sql string
	for _, j := range q.joins {
		sql += j.mode+" ."+j.table+" "+j.t+" ON "+j.t+"."+j.field+"="+q.field(j.field_foreign)+"\n"
	}
	return sql
}

func (q *query) field(s string) string {
	if q.joined && !strings.Contains(s, ".") {
		return q.t+"."+s
	}
	return s
}

func (q *query_where) where_clause(clause where_clause, value... any){
	q.where 		= append(q.where, clause)
	q.where_data 	= append(q.where_data, value...)
}

func (q *query_where) compile_where() string {
	length := len(q.where)
	if q.id != 0 {
		length++
	}
	if length == 0 {
		return ""
	}
	
	var j int
	sql := make([]string, length)
	if q.id != 0 {
		sql[j] = fmt.Sprintf("%s=%d", q.field("id"), q.id)
		j++
	}
	for i, clause := range q.where {
		sql[j] = fmt.Sprintf(clause.sql, q.field(clause.field))
		j++
		
		//	Flatten data slices
		switch v := q.where_data[i].(type) {
		case []any:
			q.data = append(q.data, v...)
		default:
			q.data = append(q.data, v)
		}
	}
	return "WHERE "+strings.Join(sql, " && ")+"\n"
}