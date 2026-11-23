package sqlc

import "fmt"

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

func (q *Delete_query) Left_join(table, t, field, field_foreign string) *Delete_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Delete_query) Where(clause *Where_clause) *Delete_query {
	q.where_clause = clause
	return q
}

func (q *Delete_query) Compile() (string, []any, error){
	if !q.use_id {
		num, _, _ := q.get_alloc()
		if num == 0 {
			return "", nil, fmt.Errorf("Delete requires a WHERE condition")
		}
	}
	
	c := compiler_pool.Get().(*compiler)
	defer c.release()
	
	if q.joined {
		c.use_alias = true
	}
	
	t := q.base_table_short()
	if err := q.compile_tables(c, t); err != nil {
		return "", nil, err
	}
	c.root_t = q.t
	
	//	Pre-allocation
	alloc := 15 + len(q.table)	//	"DELETE \n" + "FROM .\n"
	if c.use_alias {
		alloc += (1 + len(q.t)) * 2
	}
	c.sb.Alloc(alloc)
	
	var err error
	c.sb.WriteString("DELETE ")
	if c.use_alias {
		c.sb.WriteString(q.t)
		c.sb.WriteByte(' ')
	}
	q.compile_from(c)
	if err = q.compile_joins(c, nil); err != nil {
		return "", nil, err
	}
	if err = q.compile_where(c, nil); err != nil {
		return "", nil, err
	}
	
	return c.sb.String(), c.data_copy(), nil
}