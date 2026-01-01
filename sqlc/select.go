package sqlc

import (
	"strings"
	"strconv"
)

type (
	Select_query struct {
		query_where
		select_fields 	[]select_field
		select_distinct	bool
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
					table: table,
				},
			},
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
		q.select_fields[i].field = v
		if function, field, found := strings.Cut(q.select_fields[i].field, "|"); found {
			q.select_fields[i].field	= field
			q.select_fields[i].function	= function
		}
		q.select_fields[i].field, q.select_fields[i].alias, _ = strings.Cut(q.select_fields[i].field, " ")
	}
	return q
}

func (q *Select_query) Select_distinct(list []string) *Select_query {
	q.Select(list)
	q.select_distinct = true
	return q
}

func (q *Select_query) Left_join(table, t, field, field_foreign string, conditions Map) *Select_query {
	q.left_join(table, t, field, field_foreign, conditions)
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
	q.reset()
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sql_select	:= q.compile_select()
	sql_from	:= q.compile_from()
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
	alloc := len(sql_select) + len(sql_from) + len(sql_join) + len(sql_where) + len(sql_group) + len(sql_order) + len(sql_limit)
	if q.read_lock {
		alloc += 11
	}
	sb.Grow(alloc)
	
	sb.WriteString(sql_select)
	sb.WriteString(sql_from)
	if q.joined {
		sb.WriteString(sql_join)
	}
	sb.WriteString(sql_where)
	sb.WriteString(sql_group)
	sb.WriteString(sql_order)
	sb.WriteString(sql_limit)
	if q.read_lock {
		sb.WriteString("FOR UPDATE\n")
	}
	return sb.String(), nil
}

func (q *Select_query) compile_select() string {
	var sb strings.Builder
	//	Preallocation
	sb.Grow(7 + alloc_select_field * len(q.select_fields))
	
	if q.select_distinct {
		sb.WriteString("SELECT DISTINCT ")
	} else {
		sb.WriteString("SELECT ")
	}
	
	for i, s := range q.select_fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		
		field := q.field(s.field)
		if s.function != "" {
			switch s.function {
			case "sum_zero":
				sb.WriteString("IFNULL(SUM(")
				sb.WriteString(field)
				sb.WriteString("), 0)")
			default:
				sb.WriteString(strings.ToUpper(s.function))
				sb.WriteByte('(')
				sb.WriteString(field)
				sb.WriteByte(')')
			}
		} else {
			sb.WriteString(field)
		}
		
		if s.alias != "" {
			sb.WriteByte(' ')
			sb.WriteString(s.alias)
		}
	}
	sb.WriteByte('\n')
	return sb.String()
}

func (q *Select_query) compile_from() string {
	s := "FROM ."+q.table
	if q.joined {
		s += " "+q.t
	}
	return s+"\n"
}

func (q *Select_query) compile_group() string {
	if len(q.group) == 0 {
		return ""
	}
	s := "GROUP BY "
	for i, v := range q.group {
		if i > 0 {
			s += ", "
		}
		s += q.field(v)
	}
	return s+"\n"
}

func (q *Select_query) compile_order() string {
	if len(q.order) == 0 {
		return ""
	}
	s := "ORDER BY "
	for i, v := range q.order {
		if i > 0 {
			s += ", "
		}
		s += q.field(v)
	}
	return s+"\n"
}

func (q *Select_query) compile_limit() string {
	if q.limit.limit == 0 {
		return ""
	}
	return "LIMIT "+strconv.FormatUint(uint64(q.limit.offset), 10)+","+strconv.FormatUint(uint64(q.limit.limit), 10)+"\n"
}