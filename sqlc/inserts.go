package sqlc

import (
	"fmt"
	"strings"
)

type Inserts_query struct {
	query
	fields 		[]Map
	col_count	int
	col_map		Map
	col_keys	[]string
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
	f, err := q.compile_fields()
	if err != nil {
		return "", err
	}
	return s+f, nil
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

func (q *Inserts_query) compile_fields() (string, error){
	list := make([]string, len(q.fields))
	for i, fields := range q.fields {
		if q.col_count != len(fields) {
			return "", fmt.Errorf("Insert rows inconsistency")
		}
		
		row := make([]string, q.col_count)
		for i, key := range q.col_keys {
			row[i] = "?"
			q.data = append(q.data, fields[key])
		}
		list[i] = "("+strings.Join(row, ", ")+")"
	}
	return "VALUES "+strings.Join(list, ","), nil
}