package sqlc

import (
	"fmt"
	"strings"
	"strconv"
)

const (
	SELECT_RAW		= "raw"
	SELECT_SUM_ZERO	= "sum_zero"
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
		lock_for_update	bool
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

func (q *Select_query) Lock_for_update() *Select_query {
	q.lock_for_update = true
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
		if pos := strings.IndexByte(v, '='); pos != -1 {
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

func (q *Select_query) Select_json_condition(field string, query *Select_query, inner_field, outer_field string) *Select_query {
	q.select_jsons = append(q.select_jsons, &select_json{
		select_field:	field,
		query:			query,
		inner_field:	inner_field,
		outer_field:	outer_field,
	})
	return q
}

func (q *Select_query) Inner_join(table, t, field, field_foreign string) *Select_query {
	q.inner_join(table, t, field, field_foreign)
	return q
}

func (q *Select_query) Left_join(table, t, field, field_foreign string) *Select_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Select_query) Cross_join(table, t string) *Select_query {
	q.cross_join(table, t)
	return q
}

func (q *Select_query) Inner_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any) *Select_query {
	q.inner_join_fixed(table, t, field, field_foreign, field_fixed, value_fixed)
	return q
}

func (q *Select_query) Left_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any) *Select_query {
	q.left_join_fixed(table, t, field, field_foreign, field_fixed, value_fixed)
	return q
}

func (q *Select_query) Inner_join_multi(table, t string, fields Join_conditions) *Select_query {
	q.inner_join_multi(table, t, fields)
	return q
}

func (q *Select_query) Left_join_multi(table, t string, fields Join_conditions) *Select_query {
	q.left_join_multi(table, t, fields)
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
	c := compiler_pool.Get().(*compiler)
	defer func() {
		c.reset()
		compiler_pool.Put(c)
	}()
	
	var aliases alias_collect
	
	if q.joined || q.select_jsons != nil {
		c.use_alias = true
		
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
	if err := q.compile_tables(c, t); err != nil {
		return "", nil, err
	}
	c.root_t = q.t
	
	//audit := Audit(sb, "select")
	
	//	Pre-allocation
	alloc := q.alloc_field_list(len(q.select_fields), c.use_alias)
	if q.select_distinct {
		alloc += 17	//	"SELECT DISTINCT \n"
	} else {
		alloc += 8	//	"SELECT \n"
	}
	alloc += 7 + len(q.table)	//	"FROM .\n"
	if c.use_alias {
		alloc += 1 + len(q.t)
	}
	alloc += len(q.select_jsons) * alloc_query
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	var err error
	if err = q.compile_select(c); err != nil {
		return "", nil, err
	}
	q.compile_from(c)
	if err = q.compile_joins(c, aliases); err != nil {
		return "", nil, err
	}
	//audit.Audit()
	if err = q.compile_where(c, nil); err != nil {
		return "", nil, err
	}
	q.compile_group(c)
	q.compile_order(c)
	q.compile_limit(c)
	if q.lock_for_update {
		c.sb.WriteString("FOR UPDATE\n")
	}
	
	return c.sb.String(), c.data_copy(), nil
}

func (q *Select_query) collect_aliases(list alias_collect) error {
	//	Check SELECT clause
	for _, f := range q.select_fields {
		if f.function == SELECT_RAW {
			list.apply_raw(f.field)
		} else {
			list.apply(f.field)
		}
	}
	for _, f := range q.select_jsons {
		list.apply(f.inner_field)
		list.apply(f.outer_field)
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

func (q *Select_query) compile_select(c *compiler) error {
	if q.select_distinct {
		c.sb.WriteString("SELECT DISTINCT ")
	} else {
		c.sb.WriteString("SELECT ")
	}
	
	for i := range q.select_fields {
		s := &q.select_fields[i]	//	Avoid copying data
		if i > 0 {
			c.sb.WriteString(", ")
		}
		
		switch s.function {
		case "":
			c.write_field(q.t, s.field)
		case SELECT_RAW:
			if strings.Contains(s.field, ROOT_ALIAS) {
				c.sb.WriteString(strings.ReplaceAll(s.field, ROOT_ALIAS+".", q.t+"."))
			} else {
				c.sb.WriteString(s.field)
			}
		case SELECT_SUM_ZERO:
			c.sb.WriteString("IFNULL(SUM(")
			c.write_field(q.t, s.field)
			c.sb.WriteString("), 0)")
		default:
			c.sb.WriteString(strings.ToUpper(s.function))
			c.sb.WriteByte('(')
			c.write_field(q.t, s.field)
			c.sb.WriteByte(')')
		}
		
		if s.alias != "" {
			c.sb.WriteByte(' ')
			c.sb.WriteString(s.alias)
		}
	}
	
	if err := q.compile_select_joins(c); err != nil {
		return err
	}
	
	c.sb.WriteByte('\n')
	return nil
}

func (q *Select_query) compile_select_joins(c *compiler) error {
	var err error
	for _, sj := range q.select_jsons {
		if sj.query == nil {
			q.compile_select_null(c, sj)
			continue
		}
		
		if err = q.compile_select_join(c, sj); err != nil {
			return err
		}
	}
	
	return nil
}

func (q *Select_query) compile_select_null(c *compiler, sj *select_json) {
	c.sb.WriteString(", NULL ")
	c.sb.WriteString(sj.select_field)
}

func (q *Select_query) compile_select_join(c *compiler, sj *select_json) error {
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
	if err := sj.query.compile_tables(c, t); err != nil {
		return err
	}
	
	c.sb.WriteString(",\n(\nSELECT JSON_ARRAYAGG(JSON_OBJECT(")
	for i := range sj.query.select_fields {
		field := &sj.query.select_fields[i]	//	Avoid copying data
		if i > 0 {
			c.sb.WriteString(", ")
		}
		c.sb.WriteByte('\'')
		if field.alias == "" {
			if pos := strings.IndexByte(field.field, '.'); pos != -1 {
				c.sb.WriteString(field.field[pos+1:])
			} else {
				c.sb.WriteString(field.field)
			}
		} else {
			c.sb.WriteString(field.alias)
		}
		c.sb.WriteString("', ")
		c.write_field(sj.query.t, field.field)
	}
	c.sb.WriteString("))\n")
	
	sj.query.compile_from(c)
	if err := sj.query.compile_joins(c, sub_aliases); err != nil {
		return err
	}
	
	if err := sj.query.compile_where(c, func(c *compiler, first *bool){
		if sj.inner_field == "" {
			return
		}
		
		if *first {
			*first = false
		} else {
			c.sb.WriteString(" AND ")
		}
		
		c.write_field(sj.query.t, sj.inner_field)
		c.sb.WriteByte('=')
		c.write_field(q.t, sj.outer_field)
	}); err != nil {
		return err
	}
	
	sj.query.compile_group(c)
	sj.query.compile_order(c)
	sj.query.compile_limit(c)
	
	c.sb.WriteString(") ")
	c.sb.WriteString(sj.select_field)
	
	return nil
}

func (q *Select_query) compile_group(c *compiler){
	length := len(q.group)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	c.sb.Alloc(10 + q.alloc_field_list(length, c.use_alias))
	
	c.sb.WriteString("GROUP BY ")
	for i, v := range q.group {
		if i > 0 {
			c.sb.WriteString(", ")
		}
		c.write_field(q.t, v)
	}
	c.sb.WriteByte('\n')
}

func (q *Select_query) compile_order(c *compiler){
	length := len(q.order)
	if length == 0 {
		return
	}
	
	//	Pre-allocation
	c.sb.Alloc(10 + q.alloc_field_list(length, c.use_alias))
	
	c.sb.WriteString("ORDER BY ")
	for i, v := range q.order {
		if i > 0 {
			c.sb.WriteString(", ")
		}
		c.write_field(q.t, v)
	}
	c.sb.WriteByte('\n')
}

func (q *Select_query) compile_limit(c *compiler){
	if q.limit.limit == 0 {
		return
	}
	
	//	Pre-allocation
	c.sb.Alloc(8 + 3 + 3)
	
	var buf [20]byte
	
	c.sb.WriteString("LIMIT ")
	c.sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.offset), 10))
	c.sb.WriteByte(',')
	c.sb.Write(strconv.AppendUint(buf[:0], uint64(q.limit.limit), 10))
	c.sb.WriteByte('\n')
}