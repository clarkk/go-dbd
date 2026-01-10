package sqlc

import (
	"fmt"
	"strings"
	"strconv"
)

type (
	Select_query struct {
		query_where
		select_fields 	[]select_field
		select_distinct	bool
		select_jsons	[]*select_json
		group			[]string
		order 			[]string
		limit 			select_limit
		read_lock		bool
	}
	
	select_field struct {
		field 			string
		function		string
		alias 			string
	}
	
	select_limit struct {
		offset 			uint32
		limit 			uint8
	}
	
	select_json struct {
		select_field	string
		query			*Select_query
		inner_field		string
		outer_field		string
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
		f := &q.select_fields[i]
		
		if pos := strings.IndexByte(v, '|'); pos != -1 {
			f.function = v[:pos]
			v = v[pos+1:]
		}
		if pos := strings.IndexByte(v, ' '); pos != -1 {
			f.field = v[:pos]
			f.alias = v[pos+1:]
		} else {
			f.field = v
		}
	}
	return q
}

func (q *Select_query) Select_distinct(list []string) *Select_query {
	q.Select(list)
	q.select_distinct = true
	return q
}

func (q *Select_query) Select_json(field string, query *Select_query, inner_field, outer_field string) *Select_query {
	q.select_jsons = append(q.select_jsons, &select_json{
		select_field:	field,
		query:			query,
		inner_field:	inner_field,
		outer_field:	outer_field,
	})
	return q
}

func (q *Select_query) Left_join(table, t, field, field_foreign string, conditions Map) *Select_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Select_query) Where(clause *Where_clause) *Select_query {
	q.where_clause = clause
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
	
	sb := builder_pool.Get().(*sbuilder)
	defer func() {
		sb.Reset()
		builder_pool.Put(sb)
	}()
	
	//audit := Audit(sb, "select")
	
	//	Pre-allocation
	alloc := q.alloc_field_list(len(q.select_fields))
	if q.select_distinct {
		alloc += 17	//	"SELECT DISTINCT \n"
	} else {
		alloc += 8	//	"SELECT \n"
	}
	alloc += 7 + len(q.table)	//	"FROM .\n"
	if q.joined {
		alloc += 1 + len(q.t)
	}
	alloc += len(q.select_jsons) * alloc_query
	sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	if err := q.compile_select(sb); err != nil {
		return "", err
	}
	q.compile_from(sb)
	q.compile_joins(sb)
	//audit.Audit()
	if err := q.compile_where(sb, nil); err != nil {
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

func (q *Select_query) compile_select(sb *sbuilder) error {
	if q.select_distinct {
		sb.WriteString("SELECT DISTINCT ")
	} else {
		sb.WriteString("SELECT ")
	}
	
	for i := range q.select_fields {
		s := &q.select_fields[i]	//	Avoid copying data
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
	
	if err := q.compile_select_joins(sb); err != nil {
		return err
	}
	
	sb.WriteByte('\n')
	return nil
}

func (q *Select_query) compile_select_joins(sb *sbuilder) error {
	for _, sj := range q.select_jsons {
		if len(sj.query.select_fields) < 2 {
			return fmt.Errorf("Minimum 2 fields in select json")
		}
		
		t := sj.query.base_table_short()
		if err := sj.query.compile_tables(t); err != nil {
			return err
		}
		
		sb.WriteString(",\n(\nSELECT JSON_OBJECTAGG(")
		sj.query.write_field(sb, sj.query.select_fields[0].field)
		sb.WriteString(", JSON_OBJECT(")
		for i, field := range sj.query.select_fields[1:] {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('\'')
			sb.WriteString(field.alias)
			sb.WriteString("', ")
			sj.query.write_field(sb, field.field)
		}
		sb.WriteString("))\n")
		
		sj.query.compile_from(sb)
		sj.query.compile_joins(sb)
		
		if err := sj.query.compile_where(sb, func(sb_inner *sbuilder, first *bool){
			if *first {
				*first = false
			} else {
				sb_inner.WriteString(" AND ")
			}
			
			sj.query.write_field(sb_inner, sj.inner_field)
			sb_inner.WriteByte('=')
			q.write_field(sb_inner, sj.outer_field)
		}); err != nil {
			return err
		}
		
		sj.query.compile_group(sb)
		sj.query.compile_order(sb)
		sj.query.compile_limit(sb)
		
		sb.WriteString(") ")
		sb.WriteString(sj.select_field)
		
		q.append_data(sj.query.Data())
	}
	
	return nil
}

func (q *Select_query) compile_group(sb *sbuilder){
	length := len(q.group)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Alloc(10 + q.alloc_field_list(length))
	
	sb.WriteString("GROUP BY ")
	for i, v := range q.group {
		if i > 0 {
			sb.WriteString(", ")
		}
		q.write_field(sb, v)
	}
	sb.WriteByte('\n')
}

func (q *Select_query) compile_order(sb *sbuilder){
	length := len(q.order)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Alloc(10 + q.alloc_field_list(length))
	
	sb.WriteString("ORDER BY ")
	for i, v := range q.order {
		if i > 0 {
			sb.WriteString(", ")
		}
		q.write_field(sb, v)
	}
	sb.WriteByte('\n')
}

func (q *Select_query) compile_limit(sb *sbuilder){
	if q.limit.limit == 0 {
		return
	}
	
	//	Pre-allocation
	sb.Alloc(8 + 3 + 3)
	
	var buf [20]byte
	
	sb.WriteString("LIMIT ")
	sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.offset), 10))
	sb.WriteByte(',')
	sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.limit), 10))
	sb.WriteByte('\n')
}