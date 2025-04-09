package sqlc

import (
	"strings"
	"strconv"
)

type (
	Select_query struct {
		query_where
		select_fields 	[]select_field
		json_table		*json_table
		group			[]string
		order 			[]string
		limit 			select_limit
		read_lock		bool
	}
	
	select_field struct {
		field 		string
		function	string
		alias 		string
	}
	
	select_limit struct {
		offset 		uint32
		limit 		uint8
	}
)

func Select_id(table string, id uint64) *Select_query {
	q := Select(table)
	q.use_id 	= true
	q.id 		= id
	return q
}

func Select(table string) *Select_query {
	return &Select_query{
		query_where: query_where{
			query_join: query_join{
				query: query{
					table:		table,
					data:		[]any{},
				},
				joins: 		[]join{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
	}
}

func (q *Select_query) Read_lock() *Select_query {
	q.read_lock = true
	return q
}

func (q *Select_query) Select(list []string) *Select_query {
	q.select_fields = make([]select_field, len(list))
	for i, v := range list {
		s := select_field{
			field: v,
		}
		if function, field, found := strings.Cut(s.field, "|"); found {
			s.field		= field
			s.function	= function
		}
		s.field, s.alias, _ = strings.Cut(s.field, " ")
		q.select_fields[i] = s
	}
	return q
}

func (q *Select_query) JSON_table(j *json_table) *Select_query {
	q.json_table = j
	return q
}

func (q *Select_query) Left_join(table, t, field, field_foreign string) *Select_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Select_query) Where(clauses *Where_clause) *Select_query {
	clauses.apply(q)
	return q
}

func (q *Select_query) Group(fields []string) *Select_query {
	q.group = fields
	return q
}

func (q *Select_query) Order(fields []string) *Select_query {
	q.order = fields
	return q
}

func (q *Select_query) Limit(offset uint32, limit uint8) *Select_query {
	q.limit = select_limit{offset, limit}
	return q
}

func (q *Select_query) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_select()+q.compile_from()
	if q.joined {
		s += q.compile_joins()
	}
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	s += sql_where+q.compile_group()+q.compile_order()
	if q.limit.limit != 0 {
		s += q.compile_limit()
	}
	if q.read_lock {
		s += "FOR UPDATE\n"
	}
	return s, nil
}

func (q *Select_query) compile_select() string {
	list := make([]string, len(q.select_fields))
	for i, s := range q.select_fields {
		list[i] = q.field(s.field)
		if s.function != "" {
			list[i] = s.function+"("+list[i]+")"
		}
		if s.alias != "" {
			list[i] += " "+s.alias
		}
	}
	return "SELECT "+strings.Join(list, ", ")+"\n"
}

func (q *Select_query) compile_from() string {
	s := "FROM ."+q.table
	if q.joined {
		s += " "+q.t
	}
	if q.json_table != nil {
		s += q.json_table.compile()
	}
	return s+"\n"
}

func (q *Select_query) compile_group() string {
	if len(q.group) == 0 {
		return ""
	}
	for i, v := range q.group {
		q.group[i] = q.field(v)
	}
	return "GROUP BY "+strings.Join(q.group, ", ")+"\n"
}

func (q *Select_query) compile_order() string {
	if len(q.order) == 0 {
		return ""
	}
	for i, v := range q.order {
		q.order[i] = q.field(v)
	}
	return "ORDER BY "+strings.Join(q.order, ", ")+"\n"
}

func (q *Select_query) compile_limit() string {
	return "LIMIT "+strconv.FormatUint(uint64(q.limit.offset), 10)+","+strconv.FormatUint(uint64(q.limit.limit), 10)+"\n"
}