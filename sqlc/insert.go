package sqlc

import (
	"strings"
	"slices"
)

type Insert_query struct {
	query_join
	fields 				Map
	update_duplicate	bool
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

func (q *Insert_query) Update_duplicate() *Insert_query {
	q.update_duplicate = true
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
		s += "ON DUPLICATE KEY UPDATE "+sql+"\n"
		q.data = slices.Concat(q.data, data)
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
	sql := make([]string, len(q.fields))
	data := make([]any, len(q.fields))
	i := 0
	for k, v := range q.fields {
		sql[i]	= q.field(k)+"=?"
		data[i] = v
		i++
	}
	return strings.Join(sql, ", "), data
}