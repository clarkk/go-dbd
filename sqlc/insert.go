package sqlc

import "fmt"

type Insert_query struct {
	query_join
	fields 						*Fields_clause
	update_duplicate			bool
	update_duplicate_fields 	[]string
	map_fields					map[string]int
}

func Insert(table string) *Insert_query {
	return &Insert_query{
		query_join: query_join{
			query: query{
				table: table,
			},
		},
		map_fields: map[string]int{},
	}
}

func (q *Insert_query) Update_duplicate(update_fields []string) *Insert_query {
	q.update_duplicate			= true
	q.update_duplicate_fields	= update_fields
	return q
}

func (q *Insert_query) Update_duplicate_operator(fields *Fields_clause, update_fields []string) *Insert_query {
	q.fields 					= fields
	q.update_duplicate			= true
	q.update_duplicate_fields	= update_fields
	return q
}

func (q *Insert_query) Fields(fields map[string]any) *Insert_query {
	q.fields = fields_from_map(fields)
	return q
}

func (q *Insert_query) Left_join(table, t, field, field_foreign string) *Insert_query {
	q.left_join(table, t, field, field_foreign)
	return q
}

func (q *Insert_query) Compile() (string, []any, error){
	if q.fields == nil || len(q.fields.entries) == 0 {
		return "", nil, fmt.Errorf("Insert requires at least one field")
	}
	
	c := compiler_pool.Get().(*compiler)
	defer c.release()
	
	t := q.base_table_short()
	if err := q.compile_tables(c, t); err != nil {
		return "", nil, err
	}
	c.root_t = q.t
	
	//audit := Audit(sb, "insert")
	
	//	Pre-allocation
	alloc := 14 + len(q.table) + alloc_field_assign(len(q.fields.entries))	//	"INSERT .\n" + "SET \n"
	if q.update_duplicate {
		alloc += 25	//	"ON DUPLICATE KEY UPDATE \n"
		if q.update_duplicate_fields != nil {
			alloc += alloc_field_assign(len(q.update_duplicate_fields))
		} else {
			alloc += alloc_field_assign(len(q.fields.entries))
		}
	}
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	c.sb.WriteString("INSERT .")
	c.sb.WriteString(q.table)
	c.sb.WriteByte('\n')
	c.sb.WriteString("SET ")
	if err := q.compile_fields(c); err != nil {
		return "", nil, err
	}
	c.sb.WriteByte('\n')
	if q.update_duplicate {
		c.sb.WriteString("ON DUPLICATE KEY UPDATE ")
		err := q.compile_update_duplicate_fields(c)
		if err != nil {
			return "", nil, err
		}
		c.sb.WriteByte('\n')
	}
	//audit.Audit()
	
	return c.sb.String(), c.data_copy(), nil
}

func (q *Insert_query) compile_fields(c *compiler) error {
	clear(q.map_fields)
	c.alloc_data_capacity(len(q.fields.entries))
	
	for i, entry := range q.fields.entries {
		if _, found := q.map_fields[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		q.map_fields[entry.field] = i
		
		if i > 0 {
			c.sb.WriteString(", ")
		}
		c.write_field(q.t, entry.field)
		c.sb.WriteString("=?")
		c.data = append(c.data, entry.value)
	}
	return nil
}

func (q *Insert_query) compile_update_duplicate_fields(c *compiler) error {
	if q.update_duplicate_fields != nil {
		length := len(q.update_duplicate_fields)
		
		c.alloc_data_capacity(length)
		
		for i, field := range q.update_duplicate_fields {
			j, found := q.map_fields[field]
			if !found {
				return fmt.Errorf("Invalid field: %s", field)
			}
			
			if i > 0 {
				c.sb.WriteString(", ")
			}
			
			q.write_update_field(c, field, q.fields.entries[j].operator, q.fields.entries[j].value)
		}
	} else {
		length := len(q.fields.entries)
		
		c.alloc_data_capacity(length)
		
		for i, entry := range q.fields.entries {
			if i > 0 {
				c.sb.WriteString(", ")
			}
			
			q.write_update_field(c, entry.field, entry.operator, entry.value)
		}
	}
	return nil
}