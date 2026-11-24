package sqlc

import (
	"fmt"
	"slices"
)

type Inserts_query struct {
	query_join
	fields 					[][]any
	update_duplicate		bool
	update_duplicate_fields []string
	col_count				int
	col_map					map[string]int
	col_keys				[]string
}

func Inserts(table string) *Inserts_query {
	return &Inserts_query{
		query_join: query_join{
			query: query{
				table: table,
			},
		},
	}
}

func (q *Inserts_query) Update_duplicate(update_fields []string) *Inserts_query {
	q.update_duplicate			= true
	q.update_duplicate_fields	= update_fields
	return q
}

func (q *Inserts_query) Fields(fields map[string]any) error {
	length := len(fields)
	if len(q.fields) > 0 && length != q.col_count {
		return fmt.Errorf("Invalid insert consistency: row %d has %d fields, expected %d", len(q.fields)+1, length, q.col_count)
	}
	
	if len(q.fields) == 0 {
		q.col_count	= length
		q.col_map	= make(map[string]int, length)
		//	Sort keys
		q.col_keys = make([]string, length)
		var i int
		for key := range fields {
			q.col_keys[i] = key
			i++
		}
		slices.Sort(q.col_keys)
		for i, key := range q.col_keys {
			q.col_map[key] = i
		}
	}
	
	row := make([]any, q.col_count)
	for key, value := range fields {
		i, ok := q.col_map[key]
		if !ok {
			return fmt.Errorf("Invalid insert field: %s", key)
		}
		row[i] = value
	}
	
	q.fields = append(q.fields, row)
	return nil
}

func (q *Inserts_query) Left_join(table, t, field, field_foreign string) *Inserts_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Inserts_query) Compile() (string, []any, error){
	if q.update_duplicate_fields != nil && len(q.update_duplicate_fields) == 0 {
		return "", nil, fmt.Errorf("Update_duplicate requires at least one field")
	}
	
	c := compiler_pool.Get().(*compiler)
	defer c.release()
	
	t := q.base_table_short()
	if err := q.compile_tables(c, t); err != nil {
		return "", nil, err
	}
	c.root_t = q.t
	
	//audit := Audit(sb, "inserts")
	
	//	Pre-allocation
	alloc := 20 + len(q.table) + q.alloc_field_list(q.col_count, c.use_alias)	//	"INSERT ." + " ()\nVALUES \n"
	alloc += len(q.fields) * (3 + alloc_field_placeholder_list(q.col_count))	//	"(),"
	if q.update_duplicate {
		alloc += 25										//	"ON DUPLICATE KEY UPDATE \n"
		alloc += q.col_count * (9 + 2 * alloc_field)	//	"=VALUES()"
	}
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	q.compile_inserts(c)
	c.sb.WriteString("VALUES ")
	if err := q.compile_fields(c); err != nil {
		return "", nil, err
	}
	c.sb.WriteByte('\n')
	
	if q.update_duplicate {
		c.sb.WriteString("ON DUPLICATE KEY UPDATE ")
		
		if q.update_duplicate_fields != nil {
			var found bool
			for i, field := range q.update_duplicate_fields {
				if _, found = q.col_map[field]; !found {
					return "", nil, fmt.Errorf("Invalid update duplicate field: %s", field)
				}
				if i > 0 {
					c.sb.WriteByte(',')
				}
				q.write_update_duplicate_field(c, field)
			}
		} else {
			for i, field := range q.col_keys {
				if i > 0 {
					c.sb.WriteByte(',')
				}
				q.write_update_duplicate_field(c, field)
			}
		}
		
		c.sb.WriteByte('\n')
	}
	//audit.Audit()
	
	return c.sb.String(), c.data_copy(), nil
}

func (q *Inserts_query) compile_inserts(c *compiler){
	c.sb.WriteString("INSERT .")
	c.sb.WriteString(q.table)
	c.sb.WriteString(" (")
	for i, k := range q.col_keys {
		if i > 0 {
			c.sb.WriteString(",")
		}
		c.write_field(q.t, k)
	}
	c.sb.WriteString(")\n")
}

func (q *Inserts_query) compile_fields(c *compiler) error {
	length := len(q.fields)
	if length == 0 {
		return fmt.Errorf("No rows to insert")
	}
	
	c.alloc_data_capacity(q.col_count * length)
	
	for i, row := range q.fields {
		if i > 0 {
			c.sb.WriteByte(',')
		}
		
		c.sb.WriteByte('(')
		c.write_field_placeholder_list(q.col_count)
		c.sb.WriteByte(')')
		c.data = append(c.data, row...)
	}
	return nil
}

func (q *Inserts_query) write_update_duplicate_field(c *compiler, field string){
	c.write_field(q.t, field)
	c.sb.WriteString("=VALUES(")
	c.write_field(q.t, field)
	c.sb.WriteByte(')')
}