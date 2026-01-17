package sqlc

type Delete_query struct {
	query_where
}

func Delete_id(table string, id uint64) *Delete_query {
	q := Delete(table)
	q.use_id 	= true
	q.id 		= id
	return q
}

func Delete(table string) *Delete_query {
	return &Delete_query{
		query_where: query_where{
			query_join: query_join{
				query: query{
					table: table,
				},
			},
		},
	}
}

func (q *Delete_query) Left_join(table, t, field, field_foreign string, conditions Map) *Delete_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Delete_query) Where(clause *Where_clause) *Delete_query {
	q.where_clause = clause
	return q
}

func (q *Delete_query) Compile() (string, []any, error){
	ctx := compiler_pool.Get().(*compiler)
	ctx.t = q.t
	defer func() {
		ctx.reset()
		compiler_pool.Put(ctx)
	}()
	
	if q.joined {
		ctx.use_alias = true
	}
	
	t := q.base_table_short()
	if err := q.compile_tables(ctx, t); err != nil {
		return "", nil, err
	}
	
	//	Pre-allocation
	alloc := 15 + len(q.table)	//	"DELETE \n" + "FROM .\n"
	if ctx.use_alias {
		alloc += (1 + len(q.t)) * 2
	}
	ctx.sb.Alloc(alloc)
	
	ctx.sb.WriteString("DELETE ")
	if ctx.use_alias {
		ctx.sb.WriteString(q.t)
		ctx.sb.WriteByte(' ')
	}
	q.compile_from(ctx)
	q.compile_joins(ctx, nil)
	if err := q.compile_where(ctx, nil); err != nil {
		return "", nil, err
	}
	
	return ctx.sb.String(), ctx.data, nil
}