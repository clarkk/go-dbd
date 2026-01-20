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

func (q *Select_query) Optimize_joins() *Select_query {
	q.optimize_joins = true
	return q
}

func (q *Select_query) Select(list []string) *Select_query {
	q.select_fields = make([]select_field, len(list))
	for i, v := range list {
		f := &q.select_fields[i]	//	Avoid copying data
		
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

func (q *Select_query) Select_json(field string, query *Select_query) *Select_query {
	q.select_jsons = append(q.select_jsons, &select_json{
		select_field:	field,
		query:			query,
	})
	return q
}

func (q *Select_query) Inner_join(table, t, field, field_foreign string, conditions Map) *Select_query {
	q.inner_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Select_query) Left_join(table, t, field, field_foreign string, conditions Map) *Select_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Select_query) Inner_join_multi(table, t string, fields Join_conditions, conditions Map) *Select_query {
	q.inner_join_multi(table, t, fields, conditions)
	return q
}

func (q *Select_query) Left_join_multi(table, t string, fields Join_conditions, conditions Map) *Select_query {
	q.left_join_multi(table, t, fields, conditions)
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

func (q *Select_query) Compile() (string, []any, error){
	ctx := compiler_pool.Get().(*compiler)
	defer func() {
		ctx.reset()
		compiler_pool.Put(ctx)
	}()
	
	var aliases alias_collect
	
	if q.joined || q.select_jsons != nil {
		ctx.use_alias = true
		
		if q.optimize_joins {
			aliases = alias_collect_pool.Get().(alias_collect)
			defer func() {
				aliases.reset()
				alias_collect_pool.Put(aliases)
			}()
			if err := q.collect_aliases(aliases); err != nil {
				return "", nil, err
			}
		}
	}
	
	t := q.base_table_short()
	if err := q.compile_tables(ctx, t); err != nil {
		return "", nil, err
	}
	ctx.root_t = q.t
	
	//audit := Audit(sb, "select")
	
	//	Pre-allocation
	alloc := q.alloc_field_list(len(q.select_fields), ctx.use_alias)
	if q.select_distinct {
		alloc += 17	//	"SELECT DISTINCT \n"
	} else {
		alloc += 8	//	"SELECT \n"
	}
	alloc += 7 + len(q.table)	//	"FROM .\n"
	if ctx.use_alias {
		alloc += 1 + len(q.t)
	}
	alloc += len(q.select_jsons) * alloc_query
	ctx.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	if err := q.compile_select(ctx); err != nil {
		return "", nil, err
	}
	q.compile_from(ctx)
	q.compile_joins(ctx, aliases)
	//audit.Audit()
	if err := q.compile_where(ctx); err != nil {
		return "", nil, err
	}
	q.compile_group(ctx)
	q.compile_order(ctx)
	q.compile_limit(ctx)
	if q.read_lock {
		ctx.sb.WriteString("FOR UPDATE\n")
	}
	
	return ctx.sb.String(), ctx.data, nil
}

func (q *Select_query) collect_aliases(list alias_collect) error {
	//	Check SELECT clause
	for _, f := range q.select_fields {
		list.apply(f.field)
	}
	
	//	Check WHERE clause
	if err := q.where_clause.collect_aliases(list); err != nil {
		return err
	}
	
	//	Check GROUP clause
	for _, f := range q.group {
		list.apply(f)
	}
	
	//	Check ORDER clause
	for _, f := range q.order {
		list.apply(f)
	}
	
	return q.resolve_alias_join_dependencies(list)
}

func (q *Select_query) compile_select(ctx *compiler) error {
	if q.select_distinct {
		ctx.sb.WriteString("SELECT DISTINCT ")
	} else {
		ctx.sb.WriteString("SELECT ")
	}
	
	for i := range q.select_fields {
		s := &q.select_fields[i]	//	Avoid copying data
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		
		if s.function != "" {
			switch s.function {
			case "sum_zero":
				ctx.sb.WriteString("IFNULL(SUM(")
				ctx.write_field(q.t, s.field)
				ctx.sb.WriteString("), 0)")
			default:
				ctx.sb.WriteString(strings.ToUpper(s.function))
				ctx.sb.WriteByte('(')
				ctx.write_field(q.t, s.field)
				ctx.sb.WriteByte(')')
			}
		} else {
			ctx.write_field(q.t, s.field)
		}
		
		if s.alias != "" {
			ctx.sb.WriteByte(' ')
			ctx.sb.WriteString(s.alias)
		}
	}
	
	if err := q.compile_select_joins(ctx); err != nil {
		return err
	}
	
	ctx.sb.WriteByte('\n')
	return nil
}

func (q *Select_query) compile_select_joins(ctx *compiler) error {
	var err error
	for _, sj := range q.select_jsons {
		if err = q.compile_select_join(ctx, sj); err != nil {
			return err
		}
	}
	
	return nil
}

func (q *Select_query) compile_select_join(ctx *compiler, sj *select_json) error {
	if len(sj.query.select_fields) < 2 {
		return fmt.Errorf("Minimum 2 fields in select json")
	}
	
	var sub_aliases alias_collect
	
	if sj.query.joined && sj.query.optimize_joins {
		sub_aliases = alias_collect_pool.Get().(alias_collect)
		defer func() {
			sub_aliases.reset()
			alias_collect_pool.Put(sub_aliases)
		}()
		if err := sj.query.collect_aliases(sub_aliases); err != nil {
			return err
		}
	}
	
	t := sj.query.base_table_short()
	if err := sj.query.compile_tables(ctx, t); err != nil {
		return err
	}
	
	ctx.sb.WriteString(",\n(\nSELECT JSON_ARRAYAGG(JSON_OBJECT(")
	for i := range sj.query.select_fields {
		field := &sj.query.select_fields[i]	//	Avoid copying data
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		ctx.sb.WriteByte('\'')
		ctx.sb.WriteString(field.alias)
		ctx.sb.WriteString("', ")
		ctx.write_field(sj.query.t, field.field)
	}
	ctx.sb.WriteString("))\n")
	
	sj.query.compile_from(ctx)
	sj.query.compile_joins(ctx, sub_aliases)
	
	if err := sj.query.compile_where(ctx); err != nil {
		return err
	}
	
	sj.query.compile_group(ctx)
	sj.query.compile_order(ctx)
	sj.query.compile_limit(ctx)
	
	ctx.sb.WriteString(") ")
	ctx.sb.WriteString(sj.select_field)
	
	return nil
}

func (q *Select_query) compile_group(ctx *compiler){
	length := len(q.group)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	ctx.sb.Alloc(10 + q.alloc_field_list(length, ctx.use_alias))
	
	ctx.sb.WriteString("GROUP BY ")
	for i, v := range q.group {
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		ctx.write_field(q.t, v)
	}
	ctx.sb.WriteByte('\n')
}

func (q *Select_query) compile_order(ctx *compiler){
	length := len(q.order)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	ctx.sb.Alloc(10 + q.alloc_field_list(length, ctx.use_alias))
	
	ctx.sb.WriteString("ORDER BY ")
	for i, v := range q.order {
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		ctx.write_field(q.t, v)
	}
	ctx.sb.WriteByte('\n')
}

func (q *Select_query) compile_limit(ctx *compiler){
	if q.limit.limit == 0 {
		return
	}
	
	//	Pre-allocation
	ctx.sb.Alloc(8 + 3 + 3)
	
	var buf [20]byte
	
	ctx.sb.WriteString("LIMIT ")
	ctx.sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.offset), 10))
	ctx.sb.WriteByte(',')
	ctx.sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.limit), 10))
	ctx.sb.WriteByte('\n')
}