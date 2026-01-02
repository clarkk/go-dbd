package sqlc

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

type Inserts_query struct {
	query_join
	fields 					[]Map
	update_duplicate		bool
	update_dublicate_fields []string
	col_count				int
	col_map					Map
	col_keys				[]string
}

func Inserts(table string) *Inserts_query {
	return &Inserts_query{
		query_join: query_join{
			query: query{
				table: table,
			},
		},
		col_map: Map{},
	}
}

func (q *Inserts_query) Update_duplicate(update_fields []string) *Inserts_query {
	q.update_duplicate			= true
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Inserts_query) Fields(fields map[string]any) *Inserts_query {
	if q.col_count == 0 {
		q.col_count	= len(fields)
		q.col_keys	= slices.Sorted(maps.Keys(fields))
	}
	for k := range fields {
		q.col_map[k] = nil
	}
	q.fields = append(q.fields, fields)
	return q
}

func (q *Inserts_query) Left_join(table, t, field, field_foreign string, conditions Map) *Inserts_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Inserts_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	sql_header, err := q.compile_inserts()
	if err != nil {
		return "", err
	}
	sql_fields, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
	var sb strings.Builder
	//	Pre-allocation
	alloc := 8 + len(sql_header) + len(sql_fields)
	if q.update_duplicate {
		alloc += 25
		if q.update_dublicate_fields != nil {
			alloc += len(q.update_dublicate_fields) * (9 + alloc_select_field)
		} else {
			alloc += q.col_count * (9 + alloc_select_field)
		}
	}
	sb.Grow(alloc)
	
	sb.WriteString(sql_header)
	sb.WriteString("VALUES ")
	sb.WriteString(sql_fields)
	sb.WriteByte('\n')
	
	if q.update_duplicate {
		sb.WriteString("ON DUPLICATE KEY UPDATE ")
		
		if q.update_dublicate_fields != nil {
			var found bool
			for i, field := range q.update_dublicate_fields {
				if _, found = q.col_map[field]; !found {
					return "", fmt.Errorf("Invalid update duplicate field: %s", field)
				}
				if i > 0 {
					sb.WriteByte(',')
				}
				q.write_update_duplicate_field(&sb, field)
			}
		} else {
			for i, field := range q.col_keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				q.write_update_duplicate_field(&sb, field)
			}
		}
		
		sb.WriteByte('\n')
	}
	return sb.String(), nil
}

func (q *Inserts_query) compile_inserts() (string, error){
	if q.col_count != len(q.col_map) {
		return "", fmt.Errorf("Insert rows inconsistency")
	}
	
	var sb strings.Builder
	//	Pre-allocation
	sb.Grow(12 + (q.col_count * alloc_select_field))
	
	sb.WriteString("INSERT .")
	sb.WriteString(q.table)
	sb.WriteString(" (")
	for i, k := range q.col_keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		q.field(&sb, k)
	}
	sb.WriteString(")\n")
	
	return sb.String(), nil
}

func (q *Inserts_query) compile_fields() (string, error){
	length	:= len(q.fields)
	q.data	= make([]any, q.col_count * length)
	
	var sb strings.Builder
	//	Pre-allocation
	sb.Grow(length * alloc_field_assignment)
	
	j := 0
	for i, fields := range q.fields {
		if q.col_count != len(fields) {
			return "", fmt.Errorf("Insert rows inconsistency in row %d", i+1)
		}
		if i > 0 {
			sb.WriteByte(',')
		}
		
		sb.WriteByte('(')
		placeholder_value_array(q.col_count, &sb)
		sb.WriteByte(')')
		
		for _, key := range q.col_keys {
			q.data[j] = fields[key]
			j++
		}
	}
	return sb.String(), nil
}

func (q *Inserts_query) write_update_duplicate_field(sb *strings.Builder, field string){
	q.field(sb, field)
	sb.WriteString("=VALUES(")
	q.field(sb, field)
	sb.WriteByte(')')
}