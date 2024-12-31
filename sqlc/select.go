package sqlc

import (
	"strings"
	"strconv"
)

type (
	Select struct {
		query_where
		select_fields 	[]select_field
		limit 			limit
	}
	
	select_field struct {
		field 		string
		alias 		string
	}
	
	limit struct {
		start 		int
		length 		int
	}
)

func NewSelect(table string) *Select {
	return &Select{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
	}
}

func (q *Select) Select(list []string) *Select {
	q.select_fields = make([]select_field, len(list))
	for i, v := range list {
		s := select_field{}
		s.field, s.alias, _ = strings.Cut(v, " ")
		q.select_fields[i] = s
	}
	return q
}

func (q *Select) Left_join(table, t, field, field_foreign string) *Select {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Select) Where(clauses *where) *Select {
	clauses.compile(q)
	return q
}

func (q *Select) Limit(start, length int) *Select {
	q.limit = limit{
		start:	start,
		length:	length,
	}
	return q
}

func (q *Select) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_select()+q.compile_from()
	if len(q.joins) != 0 {
		s += q.compile_joins()
	}
	if len(q.where) != 0 {
		s += q.compile_where()
	}
	if q.limit.start != 0 || q.limit.length != 0 {
		s += q.compile_limit()
	}
	return s, nil
}

func (q *Select) compile_select() string {
	list := make([]string, len(q.select_fields))
	for i, s := range q.select_fields {
		list[i] = q.field(s.field)
		if s.alias != "" {
			list[i] += " "+s.alias
		}
	}
	return "SELECT "+strings.Join(list, ", ")+"\n"
}

func (q *Select) compile_from() string {
	s := "FROM ."+q.table
	if q.joined {
		s += " "+q.t
	}
	return s+"\n"
}

func (q *Select) compile_limit() string {
	return "LIMIT "+strconv.Itoa(q.limit.start)+","+strconv.Itoa(q.limit.length)+"\n"
}