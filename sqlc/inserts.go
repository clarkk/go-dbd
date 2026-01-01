package sqlc

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

type Inserts_query struct {
	query
	fields 					[]Map
	update_duplicate		bool
	update_dublicate_fields []string
	col_count				int
	col_map					Map
	col_keys				[]string
}

func Inserts(table string) *Inserts_query {
	return &Inserts_query{
		query: query{
			table:		table,
			data: 		[]any{},
		},
		col_map:	Map{},
	}
}

func (q *Inserts_query) Update_duplicate(update_fields []string) *Inserts_query {
	q.update_duplicate			= true
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Inserts_query) Fields(fields map[string]any) *Inserts_query {
	if q.col_count == 0 {
		q.col_count = len(fields)
	}
	for k := range fields {
		q.col_map[k] = nil
	}
	q.fields = append(q.fields, fields)
	return q
}

func (q *Inserts_query) Compile() (string, error){
	sql_header, err := q.compile_inserts()
	if err != nil {
		return "", err
	}
	sql_fields, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(8 + len(sql_header) + len(sql_fields))
	
	sb.WriteString(sql_header)
	sb.WriteString("VALUES ")
	sb.WriteString(sql_fields)
	sb.WriteByte('\n')
	
	if q.update_duplicate {
		/*var list []string
		if q.update_dublicate_fields != nil {
			var found bool
			list = make([]string, len(q.update_dublicate_fields))
			for i, key := range q.update_dublicate_fields {
				if _, found = q.col_map[key]; !found {
					return "", fmt.Errorf("Invalid update duplicate fields")
				}
				list[i] = key+"=VALUES("+key+")"
			}
		} else {
			list = make([]string, len(q.col_keys))
			for i, key := range q.col_keys {
				list[i] = key+"=VALUES("+key+")"
			}
		}
		s += "ON DUPLICATE KEY UPDATE "+strings.Join(list, ", ")+"\n"*/
	}
	return sb.String(), nil
}

func (q *Inserts_query) compile_inserts() (string, error){
	if q.col_count != len(q.col_map) {
		return "", fmt.Errorf("Insert rows inconsistency")
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(12 + (q.col_count * alloc_select_field))
	
	sb.WriteString("INSERT .")
	sb.WriteString(q.table)
	sb.WriteString(" (")
	
	q.col_keys = slices.Sorted(maps.Keys(q.col_map))
	for i, k := range q.col_keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
	}
	
	sb.WriteString(")\n")
	
	return sb.String(), nil
}

func (q *Inserts_query) compile_fields() (string, error){
	length	:= len(q.fields)
	q.data	= make([]any, q.col_count * length)
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(length * alloc_field_assignment)
	
	j := 0
	for i, fields := range q.fields {
		if q.col_count != len(fields) {
			return "", fmt.Errorf("Insert rows inconsistency")
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