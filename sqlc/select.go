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
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sb := builder_pool.Get().(*strings.Builder)
	defer func() {
		sb.Reset()
		builder_pool.Put(sb)
	}()
	
	q.compile_select(sb)
	q.compile_from(sb)
	q.compile_joins(sb)
	if err := q.compile_where(sb); err != nil {
		return "", err
	}
	q.compile_group(sb)
	q.compile_order(sb)
	q.compile_limit(sb)
	if q.read_lock {
		sb.WriteString("FOR UPDATE\n")
	}
	
	return sb.String(), nil
}

func (q *Select_query) compile_select(sb *strings.Builder){
	//	Pre-allocation
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
		
		if s.function != "" {
			switch s.function {
			case "sum_zero":
				sb.WriteString("IFNULL(SUM(")
				q.write_field(sb, s.field)
				sb.WriteString("), 0)")
			default:
				sb.WriteString(strings.ToUpper(s.function))
				sb.WriteByte('(')
				q.write_field(sb, s.field)
				sb.WriteByte(')')
			}
		} else {
			q.write_field(sb, s.field)
		}
		
		if s.alias != "" {
			sb.WriteByte(' ')
			sb.WriteString(s.alias)
		}
	}
	sb.WriteByte('\n')
}

func (q *Select_query) compile_group(sb *strings.Builder){
	length := len(q.group)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Grow(10 + alloc_select_field * length)
	
	sb.WriteString("GROUP BY ")
	for i, v := range q.group {
		if i > 0 {
			sb.WriteString(", ")
		}
		q.write_field(sb, v)
	}
	sb.WriteByte('\n')
}

func (q *Select_query) compile_order(sb *strings.Builder){
	length := len(q.order)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Grow(10 + alloc_select_field * length)
	
	sb.WriteString("ORDER BY ")
	for i, v := range q.order {
		if i > 0 {
			sb.WriteString(", ")
		}
		q.write_field(sb, v)
	}
	sb.WriteByte('\n')
}

func (q *Select_query) compile_limit(sb *strings.Builder){
	if q.limit.limit == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Grow(8 + 3 + 3)
	
	var buf [20]byte
	
	sb.WriteString("LIMIT ")
	sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.offset), 10))
	sb.WriteByte(',')
	sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.limit), 10))
	sb.WriteByte('\n')
}