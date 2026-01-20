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
	return &Union_query{
		//	Pre-allocation with 2 queries
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

func (q *Union_query) Compile() (string, []any, error){
	ctx := compiler_pool.Get().(*compiler)
	defer func() {
		ctx.reset()
		compiler_pool.Put(ctx)
	}()
	
	if q.joined {
		ctx.use_alias = true
	}
	
	if err := q.compile_tables(ctx, "t"); err != nil {
		return "", nil, err
	}
	
	q.compile_select(ctx)
	if err := q.compile_from(ctx); err != nil {
		return "", nil, err
	}
	q.compile_joins(ctx, nil)
	if err := q.compile_where(ctx, nil); err != nil {
		return "", nil, err
	}
	q.compile_group(ctx)
	q.compile_order(ctx)
	q.compile_limit(ctx)
	ctx.sb.WriteByte('\n')
	
	return ctx.sb.String(), ctx.data, nil
}

func (q *Union_query) compile_from(ctx *compiler) error {
	length := len(q.unions)
	if length < 1 {
		return fmt.Errorf("Must have at least two queries to union")
	}
	
	sep := "UNION\n"
	if q.all {
		sep = "UNION ALL\n"
	}
	
	//audit := Audit(sb, "union")
	
	//	Pre-allocation
	alloc := 10 + len(q.t) + length * (alloc_query + len(sep))	//	"FROM (\n" + ") \n"
	ctx.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	ctx.sb.WriteString("FROM (\n")
	
	for i, query := range q.unions {
		sql, data, err := query.Compile()
		if err != nil {
			return err
		}
		
		if i > 0 {
			ctx.sb.WriteString(sep)
		}
		ctx.sb.WriteString(sql)
		
		ctx.append_data(data)
	}
	
	ctx.sb.WriteString(") ")
	ctx.sb.WriteString(q.t)
	ctx.sb.WriteByte('\n')
	//audit.Audit()
	return nil
}