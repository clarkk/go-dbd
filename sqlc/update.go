package sqlc

import (
	"fmt"
	"slices"
)

type Update_query struct {
	query_where
	fields 		*Fields_clause
}

func Update_id(table string, id uint64) *Update_query {
	q := Update(table)
	q.use_id 	= true
	q.id 		= id
	return q
}

func Update(table string) *Update_query {
	return &Update_query{
		query_where: query_where{
			query_join: query_join{
				query: query{
					table: table,
				},
			},
		},
	}
}

func (q *Update_query) Fields(fields map[string]any) *Update_query {
	q.fields = Fields()
	//	Sort keys
	keys := make([]string, len(fields))
	var i int
	for k := range fields {
		keys[i] = k
		i++
	}
	slices.Sort(keys)
	for _, field := range keys {
		q.fields.Value(field, fields[field])
	}
	return q
}

func (q *Update_query) Fields_operator(fields *Fields_clause) *Update_query {
	q.fields = fields
	return q
}

func (q *Update_query) Left_join(table, t, field, field_foreign string) *Update_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Update_query) Where(clause *Where_clause) *Update_query {
	q.where_clause = clause
	return q
}

func (q *Update_query) Compile() (string, []any, error){
	if !q.use_id {
		num, _, _ := q.get_alloc()
		if num == 0 {
			return "", nil, fmt.Errorf("Update requires a WHERE condition")
		}
	}
	
	if q.fields == nil || len(q.fields.entries) == 0 {
		return "", nil, fmt.Errorf("update requires at least one field")
	}
	
	c := compiler_pool.Get().(*compiler)
	defer c.release()
	
	if q.joined {
		c.use_alias = true
	}
	
	var err error
	t := q.base_table_short()
	if err = q.compile_tables(c, t); err != nil {
		return "", nil, err
	}
	c.root_t = q.t
	
	//audit := Audit(sb, "update")
	
	//	Pre-allocation
	alloc := 14 + len(q.table) + alloc_field_assign(len(q.fields.entries))	//	"UPDATE .\n" + "SET \n"
	if c.use_alias {
		alloc += 2 + len(q.t)
	}
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	c.sb.WriteString("UPDATE .")
	c.sb.WriteString(q.table)
	if c.use_alias {
		c.sb.WriteByte(' ')
		c.sb.WriteString(q.t)
		c.sb.WriteByte('\n')
		if err = q.compile_joins(c, nil); err != nil {
			return "", nil, err
		}
	} else {
		c.sb.WriteByte('\n')
	}
	c.sb.WriteString("SET ")
	if err = q.compile_fields(c); err != nil {
		return "", nil, err
	}
	c.sb.WriteByte('\n')
	//audit.Audit()
	if err = q.compile_where(c, nil); err != nil {
		return "", nil, err
	}
	
	return c.sb.String(), c.data_copy(), nil
}

func (q *Update_query) compile_fields(c *compiler) error {
	length := len(q.fields.entries)
	c.alloc_data_capacity(length)
	unique := make(map[string]struct{}, length)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			c.sb.WriteString(", ")
		}
		
		q.write_update_field(c, entry.field, entry.operator, entry.value)
		
		unique[entry.field]	= struct{}{}
	}
	return nil
}