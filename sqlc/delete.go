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

func (q *Delete_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sb := builder_pool.Get().(*sbuilder)
	defer func() {
		sb.Reset()
		builder_pool.Put(sb)
	}()
	
	//	Pre-allocation
	alloc := 8	//	"DELETE \n"
	if q.joined {
		alloc += 1 + len(q.t)
	}
	sb.Alloc(alloc)
	
	sb.WriteString("DELETE ")
	if q.joined {
		sb.WriteString(q.t)
		sb.WriteByte(' ')
	}
	q.compile_from(sb)
	q.compile_joins(sb)
	if err := q.compile_where(sb); err != nil {
		return "", err
	}
	
	return sb.String(), nil
}