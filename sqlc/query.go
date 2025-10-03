package sqlc

import (
	"fmt"
	"slices"
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
		t 			string
		tables 		map[string]string
		joined 		bool
		joined_t	bool
		joins 		[]join
	}
	
	query_where struct {
		query_join
		where 		[]where_clause
		where_data 	[]any
		or_groups	[]*or_group
		use_id		bool
		id 			uint64
	}
	
	or_group struct {
		where 		[]where_clause
		where_data 	[]any
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string
		join_t			string
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
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func (q *query) Data() []any {
	return q.data
}

func (q *query_join) left_join(table, t, field, field_foreign string){
	var join_t string
	if before, _, found := strings.Cut(field_foreign, "."); found {
		q.joined_t	= true
		join_t		= before
	}
	
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			"LEFT JOIN",
		table:			table,
		t:				t,
		join_t:			join_t,
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
	if q.joined_t {
		first_join	:= []join{}
		second_join	:= []join{}
		for _, j := range q.joins {
			if j.join_t == "" {
				first_join = append(first_join, j)
			} else {
				second_join = append(second_join, j)
			}
		}
		q.joins = slices.Concat(first_join, second_join)
	}
	
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

func (q *query_where) where_or_group() *or_group {
	g := &or_group{}
	q.or_groups = append(q.or_groups, g)
	return g
}

func (g *or_group) where_clause(clause where_clause, value... any){
	g.where 		= append(g.where, clause)
	g.where_data 	= append(g.where_data, value...)
}

func (q *query_where) compile_where() (string, error){
	length := len(q.where) + len(q.or_groups)
	if q.use_id {
		length++
	}
	if length == 0 {
		return "", nil
	}
	
	var j int
	sql := make([]string, length)
	if q.use_id {
		sql[j] = q.field("id")+"="+strconv.FormatUint(q.id, 10)
		j++
	}
	
	//	Apply "or groups"
	if q.or_groups != nil {
		for _, group := range q.or_groups {
			var g int
			sql_group := make([]string, len(group.where))
			for i, clause := range group.where {
				sql_group[g] = q.field(clause.field)+clause.sql
				g++
				
				//	Flatten data slices
				switch v := group.where_data[i].(type) {
				case []any:
					q.data = append(q.data, v...)
				default:
					q.data = append(q.data, v)
				}
			}
			
			sql[j] = "("+strings.Join(sql_group, " || ")+")"
			j++
		}
	}
	
	duplicates := map[string]string{}
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
		
		if clause.subquery != nil {
			sql, err := clause.subquery.Compile()
			if err != nil {
				return "", err
			}
			clause.sql = strings.Replace(clause.sql, "?", sql, 1)
		}
		
		sql[j] = q.field(clause.field)+clause.sql
		j++
		
		if clause.operator == op_null || clause.operator == op_not_null {
			continue
		}
		
		//	Apply data
		if clause.subquery != nil {
			q.data = append(q.data, clause.subquery.Data()...)
		} else {
			//	Flatten data slices
			switch v := q.where_data[i].(type) {
			case []any:
				q.data = append(q.data, v...)
			default:
				q.data = append(q.data, v)
			}
		}
	}
	return "WHERE "+strings.Join(sql, " && ")+"\n", nil
}

func where_operator_error(field, operator1, operator2 string) error {
	return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, operator1, operator2)
}