package sqlc

import (
	"fmt"
	"maps"
	"slices"
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
	keys := slices.Sorted(maps.Keys(fields))
	for _, field := range keys {
		q.fields.Value(field, fields[field])
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
	sql, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
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

func (q *Update_query) compile_fields() (string, error){
	length := len(q.fields.entries)
	q.data = make([]any, 0, length)
	unique := make(map[string]struct{}, length)
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(length * alloc_field_clause)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return "", fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		
		switch entry.operator {
		case op_update_add:
			sb.WriteString(q.field(entry.field))
			sb.WriteByte('=')
			sb.WriteString(q.field(entry.field))
			sb.WriteString("+?")
		default:
			sb.WriteString(q.field(entry.field))
			sb.WriteString("=?")
		}
		
		q.data				= append(q.data, entry.value)
		unique[entry.field]	= struct{}{}
	}
	return sb.String(), nil
}