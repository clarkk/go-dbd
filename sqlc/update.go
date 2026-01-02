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

func (q *Update_query) Left_join(table, t, field, field_foreign string, conditions Map) *Update_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Update_query) Where(clauses *Where_clause) *Update_query {
	clauses.apply(q)
	return q
}

func (q *Update_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	var sb strings.Builder
	
	sb.WriteString("UPDATE .")
	sb.WriteString(q.table)
	if q.joined {
		sb.WriteByte(' ')
		sb.WriteString(q.t)
		sb.WriteByte('\n')
		q.compile_joins(&sb)
	} else {
		sb.WriteByte('\n')
	}
	sb.WriteString("SET ")
	if err := q.compile_fields(&sb); err != nil {
		return "", err
	}
	sb.WriteByte('\n')
	if err := q.compile_where(&sb); err != nil {
		return "", err
	}
	
	return sb.String(), nil
}

func (q *Update_query) compile_fields(sb *strings.Builder) error {
	length := len(q.fields.entries)
	q.data = make([]any, length)
	unique := make(map[string]struct{}, length)
	
	//	Pre-allocation
	sb.Grow(length * alloc_field_assignment)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		
		q.write_update_field(sb, entry.field, entry.operator)
		
		q.data[i]			= entry.value
		unique[entry.field]	= struct{}{}
	}
	return nil
}