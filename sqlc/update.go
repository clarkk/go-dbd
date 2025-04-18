package sqlc

import "strings"

type (
	Update_query struct {
		query_where
		fields 		Map
		json_remove	*json_remove
	}
	
	json_remove struct {
		json_doc	string
		json_path	string
	}
)

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
		fields: 	Map{},
	}
}

func (q *Update_query) Fields(fields map[string]any) *Update_query {
	q.fields = fields
	return q
}

func (q *Update_query) JSON_remove(json_doc, json_path string) *Update_query {
	q.json_remove = &json_remove{
		json_doc:	json_doc,
		json_path:	json_path,
	}
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
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	sql, data := q.compile_fields()
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

func (q *Update_query) compile_fields() (string, []any){
	length	:= len(q.fields)
	sql		:= make([]string, length)
	data	:= make([]any, length)
	i := 0
	for k, v := range q.fields {
		sql[i]	= q.field(k)+"=?"
		data[i] = v
		i++
	}
	if q.json_remove != nil {
		sql = append(sql, q.field(q.json_remove.json_doc)+"=JSON_REMOVE("+q.field(q.json_remove.json_doc)+", '"+q.json_remove.json_path+"')")
	}
	return strings.Join(sql, ", "), data
}