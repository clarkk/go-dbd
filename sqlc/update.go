package sqlc

import (
	"fmt"
	"strings"
)

type Update_query struct {
	query_where
	fields 		*Fields_clause
}

func Update_id(table string, id uint64) *Update_query {
	q := Update(table)
	q.use_id 	= true
	q.id 		= id
	return q
}

func Update(table string) *Update_query {
	return &Update_query{
		query_where: query_where{
			query_join: query_join{
				query: query{
					table:		table,
					data:		[]any{},
				},
				joins: 		[]join{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
	}
}

func (q *Update_query) Fields(fields map[string]any) *Update_query {
	q.fields = Fields()
	for field, value := range fields {
		q.fields.Value(field, value)
	}
	return q
}

func (q *Update_query) Fields_operator(fields *Fields_clause) *Update_query {
	q.fields = fields
	return q
}

/*func (q *Update_query) Left_join(table, t, field, field_foreign string) *Update_query {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Update_query) Where(clauses *Where_clause) *Update_query {
	clauses.apply(q)
	return q
}

func (q *Update_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	sql, data, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	q.data = data
	s := q.compile_update()+"SET "+sql+"\n"
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	s += sql_where
	return s, nil
}

func (q *Update_query) compile_update() string {
	s := "UPDATE ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Update_query) compile_fields() (string, []any, error){
	length	:= len(q.fields.fields)
	sql		:= make([]string, length)
	data	:= make([]any, length)
	unique	:= map[string]bool{}
	for i, field := range q.fields.fields {
		if _, found := unique[field]; found {
			return "", nil, fmt.Errorf("Duplicate field: %s", field)
		}
		switch operator := q.fields.operators[i]; operator {
		case op_update_add:
			sql[i]	= q.field(field)+"="+q.field(field)+"+?"
			data[i] = q.fields.values[i]
		default:
			sql[i]	= q.field(field)+"=?"
			data[i] = q.fields.values[i]
		}
		unique[field] = true
	}
	return strings.Join(sql, ", "), data, nil
}