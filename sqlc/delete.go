package sqlc

import "strings"

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

func (q *Delete_query) Where(clauses *Where_clause) *Delete_query {
	clauses.apply(q)
	return q
}

func (q *Delete_query) Compile() (string, error){
	q.reset()
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	
	var sb strings.Builder
	//	Preallocation
	sb.Grow(14 + len(q.table) + len(sql_where))
	
	sb.WriteString("DELETE FROM .")
	sb.WriteString(q.table)
	sb.WriteByte('\n')
	sb.WriteString(sql_where)
	
	return sb.String(), nil
}