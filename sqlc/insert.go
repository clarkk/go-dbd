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

/*func (q *Insert_query) Left_join(table, t, field, field_foreign string) *Insert_query {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Insert_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	sql, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	
	s := q.compile_insert()+"SET "+sql+"\n"
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	/*if q.update_duplicate {
		sql, data, err := q.compile_update_duplicate_fields()
		if err != nil {
			return "", err
		}
		s += "ON DUPLICATE KEY UPDATE "+sql+"\n"
		q.data = slices.Concat(q.data, data)
	}*/
	return s, nil
}

func (q *Insert_query) compile_insert() string {
	s := "INSERT ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Insert_query) compile_fields() (string, error){
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
		
		sb.WriteString(q.field(entry.field))
		sb.WriteString("=?")
		
		q.data						= append(q.data, entry.value)
		unique[entry.field]			= struct{}{}
		q.map_fields[entry.field]	= i
	}
	return sb.String(), nil
}

/*func (q *Insert_query) compile_update_duplicate_fields() (string, []any, error){
	if q.update_dublicate_fields != nil {
		length	:= len(q.update_dublicate_fields)
		sql		:= make([]string, length)
		data	:= make([]any, length)
		for i, field := range q.update_dublicate_fields {
			j, found := q.map_fields[field]
			if !found {
				return "", nil, fmt.Errorf("Invalid field: %s", field)
			}
			switch operator := q.fields.operators[j]; operator {
			case op_update_add:
				sql[i]	= q.field(field)+"="+q.field(field)+"+?"
				data[i] = q.fields.values[j]
			default:
				sql[i]	= q.field(field)+"=?"
				data[i] = q.fields.values[j]
			}
		}
		return strings.Join(sql, ", "), data, nil
	} else {
		length	:= len(q.fields.fields)
		sql		:= make([]string, length)
		data	:= make([]any, length)
		for i, field := range q.fields.fields {
			switch operator := q.fields.operators[i]; operator {
			case op_update_add:
				sql[i]	= q.field(field)+"="+q.field(field)+"+?"
				data[i] = q.fields.values[i]
			default:
				sql[i]	= q.field(field)+"=?"
				data[i] = q.fields.values[i]
			}
		}
		return strings.Join(sql, ", "), data, nil
	}
}*/