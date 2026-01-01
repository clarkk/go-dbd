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
					table: table,
				},
			},
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

func (q *Update_query) Where(clauses *Where_clause) *Update_query {
	clauses.apply(q)
	return q
}

func (q *Update_query) Compile() (string, error){
	q.reset()
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	sql, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(14 + len(q.table) + len(sql) + len(sql_where))
	
	sb.WriteString("UPDATE .")
	sb.WriteString(q.table)
	sb.WriteByte('\n')
	sb.WriteString("SET ")
	sb.WriteString(sql)
	sb.WriteByte('\n')
	sb.WriteString(sql_where)
	
	return sb.String(), nil
}

func (q *Update_query) compile_fields() (string, error){
	length := len(q.fields.entries)
	q.data = make([]any, length)
	unique := make(map[string]struct{}, length)
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(length * alloc_field_assignment)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return "", fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		
		q.write_update_field(&sb, q.field(entry.field), entry.operator)
		
		q.data[i]			= entry.value
		unique[entry.field]	= struct{}{}
	}
	return sb.String(), nil
}