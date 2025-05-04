package sqlc

import (
	"fmt"
	"strings"
	"slices"
)

type Insert_query struct {
	query_join
	fields 					Map
	update_duplicate		bool
	update_dublicate_fields []string
}

func Insert(table string) *Insert_query {
	return &Insert_query{
		query_join: query_join{
			query: query{
				table:		table,
				data:		[]any{},
			},
			joins: 		[]join{},
		},
		fields: 	Map{},
	}
}

func (q *Insert_query) Update_duplicate(update_fields []string) *Insert_query {
	q.update_duplicate			= true
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Insert_query) Fields(fields map[string]any) *Insert_query {
	q.fields = fields
	return q
}

/*func (q *Insert_query) Left_join(table, t, field, field_foreign string) *Insert_query {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Insert_query) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	sql, data := q.compile_fields()
	q.data = data
	s := q.compile_insert()+"SET "+sql+"\n"
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	if q.update_duplicate {
		if q.update_dublicate_fields != nil {
			sql, data, err := q.compile_update_duplicate_fields()
			if err != nil {
				return "", err
			}
			s += "ON DUPLICATE KEY UPDATE "+sql+"\n"
			q.data = slices.Concat(q.data, data)
		} else {
			s += "ON DUPLICATE KEY UPDATE "+sql+"\n"
			q.data = slices.Concat(q.data, data)
		}
	}
	return s, nil
}

func (q *Insert_query) compile_insert() string {
	s := "INSERT ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Insert_query) compile_fields() (string, []any){
	length	:= len(q.fields)
	sql		:= make([]string, length)
	data	:= make([]any, length)
	i := 0
	for k, v := range q.fields {
		sql[i]	= q.field(k)+"=?"
		data[i] = v
		i++
	}
	return strings.Join(sql, ", "), data
}

func (q *Insert_query) compile_update_duplicate_fields() (string, []any, error){
	var found bool
	length	:= len(q.update_dublicate_fields)
	sql		:= make([]string, length)
	data	:= make([]any, length)
	for i, field := range q.update_dublicate_fields {
		sql[i]			= q.field(field)+"=?"
		data[i], found	= q.fields[field]
		if !found {
			return "", nil, fmt.Errorf("Invalid update duplicate fields")
		}
	}
	return strings.Join(sql, ", "), data, nil
}