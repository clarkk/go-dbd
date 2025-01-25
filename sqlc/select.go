package sqlc

import (
	"strings"
	"strconv"
)

type (
	select_ struct {
		query_where
		select_fields 	[]select_field
		order 			[]string
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

func Select(table string, id uint64) *select_ {
	return &select_{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
			id:			id,
		},
	}
}

func (q *select_) Select(list []string) *select_ {
	q.select_fields = make([]select_field, len(list))
	for i, v := range list {
		s := select_field{}
		s.field, s.alias, _ = strings.Cut(v, " ")
		q.select_fields[i] = s
	}
	return q
}

func (q *select_) Left_join(table, t, field, field_foreign string) *select_ {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *select_) Where(clauses *Where_clause) *select_ {
	clauses.apply(q)
	return q
}

func (q *select_) Order(fields []string) *select_ {
	q.order = fields
	return q
}

func (q *select_) Limit(start, length int) *select_ {
	q.limit = limit{
		start:	start,
		length:	length,
	}
	return q
}

func (q *select_) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_select()+q.compile_from()
	if len(q.joins) != 0 {
		s += q.compile_joins()
	}
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	s += sql_where+q.compile_order()
	if q.limit.start != 0 || q.limit.length != 0 {
		s += q.compile_limit()
	}
	return s, nil
}

func (q *select_) compile_select() string {
	list := make([]string, len(q.select_fields))
	for i, s := range q.select_fields {
		list[i] = q.field(s.field)
		if s.alias != "" {
			list[i] += " "+s.alias
		}
	}
	return "SELECT "+strings.Join(list, ", ")+"\n"
}

func (q *select_) compile_from() string {
	s := "FROM ."+q.table
	if q.joined {
		s += " "+q.t
	}
	return s+"\n"
}

func (q *select_) compile_order() string {
	if len(q.order) == 0 {
		return ""
	}
	for i, v := range q.order {
		q.order[i] = q.field(v)
	}
	return "ORDER BY "+strings.Join(q.order, ", ")+"\n"
}

func (q *select_) compile_limit() string {
	return "LIMIT "+strconv.Itoa(q.limit.start)+","+strconv.Itoa(q.limit.length)+"\n"
}