package sqlc

import (
	"fmt"
	"maps"
	"strings"
	"slices"
)

type Insert_query struct {
	query_join
	fields 						*Fields_clause
	update_duplicate			bool
	update_dublicate_fields 	[]string
	map_fields					map[string]int
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
		map_fields: map[string]int{},
	}
}

func (q *Insert_query) Update_duplicate(update_fields []string) *Insert_query {
	q.update_duplicate			= true
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Insert_query) Update_duplicate_operator(fields *Fields_clause, update_fields []string) *Insert_query {
	q.fields 					= fields
	q.update_duplicate			= true
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Insert_query) Fields(fields map[string]any) *Insert_query {
	q.fields = Fields()
	keys := slices.Sorted(maps.Keys(fields))
	for _, field := range keys {
		q.fields.Value(field, fields[field])
	}
	return q
}

func (q *Insert_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	sql, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(14 + len(q.table) + len(sql))
	
	sb.WriteString("INSERT .")
	sb.WriteString(q.table)
	sb.WriteByte('\n')
	sb.WriteString("SET ")
	sb.WriteString(sql)
	sb.WriteByte('\n')
	
	if q.update_duplicate {
		sql_update, data, err := q.compile_update_duplicate_fields()
		if err != nil {
			return "", err
		}
		//	Preallocation
		sb.Grow(25 + len(sql_update))
		
		sb.WriteString("ON DUPLICATE KEY UPDATE ")
		sb.WriteString(sql_update)
		sb.WriteByte('\n')
		
		q.data = slices.Concat(q.data, data)
	}
	return sb.String(), nil
}

func (q *Insert_query) compile_fields() (string, error){
	length := len(q.fields.entries)
	q.data = make([]any, length)
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
		
		sb.WriteString(q.field(entry.field))
		sb.WriteString("=?")
		
		q.data[i]					= entry.value
		unique[entry.field]			= struct{}{}
		q.map_fields[entry.field]	= i
	}
	return sb.String(), nil
}

func (q *Insert_query) compile_update_duplicate_fields() (string, []any, error){
	var (
		sb		strings.Builder
		data	[]any
	)
	
	if q.update_dublicate_fields != nil {
		length	:= len(q.update_dublicate_fields)
		data	= make([]any, length)
		
		//	Preallocation
		sb.Grow(length * alloc_field_clause)
		
		for i, field := range q.update_dublicate_fields {
			j, found := q.map_fields[field]
			if !found {
				return "", nil, fmt.Errorf("Invalid field: %s", field)
			}
			
			if i > 0 {
				sb.WriteString(", ")
			}
			
			q.write_update_field(&sb, q.field(field), q.fields.entries[j].operator)
			
			data[i] = q.fields.entries[j].value
		}
	} else {
		length	:= len(q.fields.entries)
		data	= make([]any, length)
		
		//	Preallocation
		sb.Grow(length * alloc_field_clause)
		
		for i, entry := range q.fields.entries {
			if i > 0 {
				sb.WriteString(", ")
			}
			
			q.write_update_field(&sb, q.field(entry.field), entry.operator)
			
			data[i] = entry.value
		}
	}
	return sb.String(), data, nil
}