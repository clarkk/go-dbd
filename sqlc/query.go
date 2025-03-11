package sqlc

import (
	"fmt"
	"strconv"
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
		data 	[]any
	}
	
	query_join struct {
		query
		t 		string
		tables 	map[string]string
		joined 	bool
		joins 	[]join
	}
	
	query_where struct {
		query_join
		where 		[]where_clause
		where_data 	[]any
		id 			uint64
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string
		field 			string
		field_foreign 	string
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
	return msg+": "+SQL_debug(q)+" "+err.Error()
}

func (q *query) Data() []any {
	return q.data
}

func (q *query_join) left_join(table, t, field, field_foreign string){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			"LEFT JOIN",
		table:			table,
		t:				t,
		field:			field,
		field_foreign:	field_foreign,
	})
}

func (q *query_join) compile_tables() error {
	q.data = []any{}
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
			for i := range 26 {
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

func (q *query_join) compile_joins() string {
	var sql string
	for _, j := range q.joins {
		sql += j.mode+" ."+j.table+" "+j.t+" ON "+j.t+"."+j.field+"="+q.field(j.field_foreign)+"\n"
	}
	return sql
}

func (q *query_join) field(s string) string {
	if q.joined && !strings.Contains(s, ".") {
		return q.t+"."+s
	}
	return s
}

func (q *query_where) where_clause(clause where_clause, value... any){
	q.where 		= append(q.where, clause)
	q.where_data 	= append(q.where_data, value...)
}

func (q *query_where) compile_where() (string, error){
	length := len(q.where)
	if q.id != 0 {
		length++
	}
	if length == 0 {
		return "", nil
	}
	
	var j int
	duplicates := map[string]string{}
	sql := make([]string, length)
	if q.id != 0 {
		sql[j] = q.field("id")+"="+strconv.FormatUint(q.id, 10)
		j++
	}
	for i, clause := range q.where {
		if operator, ok := duplicates[clause.field]; ok {
			switch operator {
			//	Operator not compatable with other operators
			case op_eq, op_null, op_not_null, op_bt, op_not_bt, op_in, op_not_in:
				return "", where_operator_error(clause.field, operator, clause.operator)
			//	Operator only compatable with "oposite" operators
			case op_gt, op_gteq:
				if clause.operator != op_lt && clause.operator != op_lteq {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			case op_lt, op_lteq:
				if clause.operator != op_gt && clause.operator != op_gteq {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			}
		} else {
			duplicates[clause.field] = clause.operator
		}
		
		sql[j] = q.field(clause.field)+clause.sql
		j++
		
		if clause.operator == op_null || clause.operator == op_not_null {
			continue
		}
		
		//	Flatten data slices
		switch v := q.where_data[i].(type) {
		case []any:
			q.data = append(q.data, v...)
		default:
			q.data = append(q.data, v)
		}
	}
	return "WHERE "+strings.Join(sql, " && ")+"\n", nil
}

func where_operator_error(field, operator1, operator2 string) error {
	return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, operator1, operator2)
}