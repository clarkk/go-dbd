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
	update_duplicate_fields 	[]string
	map_fields					map[string]int
}

func Insert(table string) *Insert_query {
	return &Insert_query{
		query_join: query_join{
			query: query{
				table: table,
			},
		},
		map_fields: map[string]int{},
	}
}

func (q *Insert_query) Update_duplicate(update_fields []string) *Insert_query {
	q.update_duplicate			= true
	q.update_duplicate_fields	= update_fields
	return q
}

func (q *Insert_query) Update_duplicate_operator(fields *Fields_clause, update_fields []string) *Insert_query {
	q.fields 					= fields
	q.update_duplicate			= true
	q.update_duplicate_fields	= update_fields
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

func (q *Insert_query) Left_join(table, t, field, field_foreign string, conditions Map) *Insert_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Insert_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sb := builder_pool.Get().(*strings.Builder)
	defer func() {
		sb.Reset()
		builder_pool.Put(sb)
	}()
	
	//	Pre-allocation
	alloc := 14 + len(q.table) + len(q.fields.entries) * alloc_field_assignment	//	"INSERT .\n" + "SET \n"
	if q.update_duplicate {
		alloc += 25	//	"ON DUPLICATE KEY UPDATE \n"
		if q.update_duplicate_fields != nil {
			alloc += len(q.update_duplicate_fields) * alloc_field_assignment
		} else {
			alloc += len(q.fields.entries) * alloc_field_assignment
		}
	}
	sb.Grow(alloc)
	
	sb.WriteString("INSERT .")
	sb.WriteString(q.table)
	sb.WriteByte('\n')
	sb.WriteString("SET ")
	if err := q.compile_fields(sb); err != nil {
		return "", err
	}
	sb.WriteByte('\n')
	if q.update_duplicate {
		sb.WriteString("ON DUPLICATE KEY UPDATE ")
		err := q.compile_update_duplicate_fields(sb)
		if err != nil {
			return "", err
		}
		sb.WriteByte('\n')
	}
	
	return sb.String(), nil
}

func (q *Insert_query) compile_fields(sb *strings.Builder) error {
	length	:= len(q.fields.entries)
	q.data	= make([]any, length)
	unique	:= make(map[string]struct{}, length)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		
		q.write_field(sb, entry.field)
		sb.WriteString("=?")
		
		q.data[i]					= entry.value
		unique[entry.field]			= struct{}{}
		q.map_fields[entry.field]	= i
	}
	return nil
}

func (q *Insert_query) compile_update_duplicate_fields(sb *strings.Builder) error {
	if q.update_duplicate_fields != nil {
		length := len(q.update_duplicate_fields)
		
		q.alloc_data_capacity(len(q.data) + length)
		
		for i, field := range q.update_duplicate_fields {
			j, found := q.map_fields[field]
			if !found {
				return fmt.Errorf("Invalid field: %s", field)
			}
			
			if i > 0 {
				sb.WriteString(", ")
			}
			
			q.write_update_field(sb, field, q.fields.entries[j].operator)
			
			q.data = append(q.data, q.fields.entries[j].value)
		}
	} else {
		length := len(q.fields.entries)
		
		q.alloc_data_capacity(len(q.data) + length)
		
		for i, entry := range q.fields.entries {
			if i > 0 {
				sb.WriteString(", ")
			}
			
			q.write_update_field(sb, entry.field, entry.operator)
			
			q.data = append(q.data, entry.value)
		}
	}
	return nil
}