package sqlc

import (
	"fmt"
	"strings"
)

type Union_query struct {
	Select_query
	unions			[]*Select_query
	all				bool
}

func Union() *Union_query {
	return &Union_query{
		//	Preallocation with 2 queries
		unions:	make([]*Select_query, 0, 2),
	}
}

func Union_all() *Union_query {
	return &Union_query{
		//	Preallocation with 2 queries
		unions:	make([]*Select_query, 0, 2),
		all:	true,
	}
}

func (q *Union_query) Union(query *Select_query) *Union_query {
	q.unions = append(q.unions, query)
	return q
}

func (q *Union_query) Select(list []string) *Union_query {
	q.Select_query.Select(list)
	return q
}

func (q *Union_query) Select_distinct(list []string) *Union_query {
	q.Select_query.Select_distinct(list)
	return q
}

func (q *Union_query) Left_join(table, t, field, field_foreign string, conditions Map) *Union_query {
	q.Select_query.Left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Union_query) Where(clauses *Where_clause) *Union_query {
	q.Select_query.Where(clauses)
	return q
}

func (q *Union_query) Group(fields []string) *Union_query {
	q.Select_query.Group(fields)
	return q
}

func (q *Union_query) Order(fields []string) *Union_query {
	q.Select_query.Order(fields)
	return q
}

func (q *Union_query) Limit(offset uint32, limit uint8) *Union_query {
	q.Select_query.Limit(offset, limit)
	return q
}

func (q *Union_query) Compile() (string, error){
	q.reset()
	if err := q.compile_tables("t"); err != nil {
		return "", err
	}
	
	sql_select := q.compile_select()
	sql_from, err := q.compile_from()
	if err != nil {
		return "", err
	}
	sql_join := q.compile_joins()
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	sql_group	:= q.compile_group()
	sql_order	:= q.compile_order()
	sql_limit	:= q.compile_limit()
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(1 + len(sql_select) + len(sql_from) + len(sql_join) + len(sql_where) + len(sql_group) + len(sql_order) + len(sql_limit))
	
	sb.WriteString(sql_select)
	sb.WriteString(sql_from)
	if q.joined {
		sb.WriteString(sql_join)
	}
	sb.WriteString(sql_where)
	sb.WriteString(sql_group)
	sb.WriteString(sql_order)
	sb.WriteString(sql_limit)
	
	sb.WriteByte('\n')
	return sb.String(), nil
}

func (q *Union_query) compile_from() (string, error){
	length := len(q.unions)
	if length < 1 {
		return "", fmt.Errorf("Must have at least two queries to union")
	}
	
	sep := "UNION\n"
	if q.all {
		sep = "UNION ALL\n"
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(length * 200)
	
	sb.WriteString("FROM (\n")
	
	for i, query := range q.unions {
		sql, err := query.Compile()
		if err != nil {
			return "", err
		}
		
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteString(sql)
		q.data = append(q.data, query.Data()...)
	}
	
	sb.WriteString(") ")
	sb.WriteString(q.t)
	sb.WriteByte('\n')
	return sb.String(), nil
}