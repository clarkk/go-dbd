package sqlc

import (
	"fmt"
)

type Union_query struct {
	Select_query
	unions			[]*Select_query
	all				bool
}

func Union() *Union_query {
	return &Union_query{
		//	Pre-allocation with 2 queries
		unions:	make([]*Select_query, 0, 2),
	}
}

func Union_all() *Union_query {
	q := Union()
	q.all = true
	return q
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

func (q *Union_query) Left_join(table, t, field, field_foreign string) *Union_query {
	q.Select_query.Left_join(table, t, field, field_foreign)
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

func (q *Union_query) Compile() (string, []any, error){
	c := compiler_pool.Get().(*compiler)
	defer c.release()
	
	if q.joined {
		c.use_alias = true
	}
	
	var err error
	if err = q.compile_tables(c, "t"); err != nil {
		return "", nil, err
	}
	
	if err = q.compile_select(c); err != nil {
		return "", nil, err
	}
	if err = q.compile_from(c); err != nil {
		return "", nil, err
	}
	if err = q.compile_joins(c, nil); err != nil {
		return "", nil, err
	}
	if err = q.compile_where(c, nil); err != nil {
		return "", nil, err
	}
	q.compile_field_clause(c, "GROUP BY ", q.group)
	q.compile_field_clause(c, "ORDER BY ", q.order)
	q.compile_limit(c)
	c.sb.WriteByte('\n')
	
	return c.sb.String(), c.data_copy(), nil
}

func (q *Union_query) compile_from(c *compiler) error {
	length := len(q.unions)
	if length < 2 {
		return fmt.Errorf("Must have at least two queries to union")
	}
	
	sep := "UNION\n"
	if q.all {
		sep = "UNION ALL\n"
	}
	
	//audit := Audit(sb, "union")
	
	//	Pre-allocation
	alloc := 10 + len(q.t) + length * (alloc_query + len(sep))	//	"FROM (\n" + ") \n"
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	c.sb.WriteString("FROM (\n")
	
	for i, query := range q.unions {
		if query == nil {
			return fmt.Errorf("Union query %d is nil", i+1)
		}
		
		sql, data, err := query.Compile()
		if err != nil {
			return err
		}
		
		if i > 0 {
			c.sb.WriteString(sep)
		}
		c.sb.WriteString(sql)
		c.alloc_data_capacity(len(data))
		c.data = append(c.data, data...)
	}
	
	c.sb.WriteString(") ")
	c.sb.WriteString(q.t)
	c.sb.WriteByte('\n')
	//audit.Audit()
	return nil
}