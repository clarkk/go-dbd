package sqlc

import (
	"fmt"
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
	s, err := q.compile_inserts()
	if err != nil {
		return "", err
	}
	sql, data, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	s += "VALUES "+sql+"\n"
	q.data = data
	if q.update_duplicate {
		var list []string
		if q.update_dublicate_fields != nil {
			list = make([]string, len(q.update_dublicate_fields))
			for i, key := range q.update_dublicate_fields {
				list[i] = key+"=VALUES("+key+")"
			}
		} else {
			list = make([]string, len(q.col_keys))
			for i, key := range q.col_keys {
				list[i] = key+"=VALUES("+key+")"
			}
		}
		s += "ON DUPLICATE KEY UPDATE "+strings.Join(list, ", ")+"\n"
	}
	return s, nil
}

func (q *Inserts_query) compile_inserts() (string, error){
	if q.col_count != len(q.col_map) {
		return "", fmt.Errorf("Insert rows inconsistency")
	}
	q.col_keys = make([]string, q.col_count)
	i := 0
	for k := range q.col_map {
		q.col_keys[i] = k
		i++
	}
	return "INSERT ."+q.table+" ("+strings.Join(q.col_keys, ", ")+")\n", nil
}

func (q *Inserts_query) compile_fields() (string, []any, error){
	length	:= len(q.fields)
	sql		:= make([]string, length)
	data	:= make([]any, length * q.col_count)
	j := 0
	for i, fields := range q.fields {
		if q.col_count != len(fields) {
			return "", nil, fmt.Errorf("Insert rows inconsistency")
		}
		
		row := make([]string, q.col_count)
		for i, key := range q.col_keys {
			row[i]	= "?"
			data[j]	= fields[key]
			j++
		}
		sql[i] = "("+strings.Join(row, ", ")+")"
	}
	return strings.Join(sql, ","), data, nil
}