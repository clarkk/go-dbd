package sqlc

import (
	"fmt"
	"maps"
	"slices"
)

type Inserts_query struct {
	query_join
	fields 					[][]any
	update_duplicate		bool
	update_dublicate_fields []string
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
	q.update_dublicate_fields	= update_fields
	return q
}

func (q *Inserts_query) Fields(fields map[string]any) error {
	length := len(fields)
	if q.col_count == 0 {
		q.col_count	= length
		q.col_map	= make(map[string]int, q.col_count)
		q.col_keys	= slices.Sorted(maps.Keys(fields))
		for i, key := range q.col_keys {
			q.col_map[key] = i
		}
	} else {
		if length != q.col_count {
			return fmt.Errorf("Invalid insert consistency: row %d has %d fields, expected %d", len(q.fields)+1, length, q.col_count)
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

func (q *Inserts_query) Left_join(table, t, field, field_foreign string, conditions Map) *Inserts_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Inserts_query) Compile() (string, error){
	t := q.base_table_short()
	if err := q.compile_tables(t); err != nil {
		return "", err
	}
	
	sb := builder_pool.Get().(*sbuilder)
	defer func() {
		sb.Reset()
		builder_pool.Put(sb)
	}()
	
	//audit := Audit(sb, "inserts")
	
	//	Pre-allocation
	alloc := 20 + len(q.table) + alloc_field_list(q.col_count)					//	"INSERT ." + " ()\nVALUES \n"
	alloc += len(q.fields) * (3 + alloc_field_placeholder_list(q.col_count))	//	"(),"
	if q.update_duplicate {
		alloc += 25										//	"ON DUPLICATE KEY UPDATE \n"
		alloc += q.col_count * (9 + 2 * alloc_field)	//	"=VALUES()"
	}
	sb.Alloc(alloc)
	//audit.Grow(alloc)
	q.compile_inserts(sb)
	sb.WriteString("VALUES ")
	if err := q.compile_fields(sb); err != nil {
		return "", err
	}
	sb.WriteByte('\n')
	
	if q.update_duplicate {
		sb.WriteString("ON DUPLICATE KEY UPDATE ")
		
		if q.update_dublicate_fields != nil {
			var found bool
			for i, field := range q.update_dublicate_fields {
				if _, found = q.col_map[field]; !found {
					return "", fmt.Errorf("Invalid update duplicate field: %s", field)
				}
				if i > 0 {
					sb.WriteByte(',')
				}
				q.write_update_duplicate_field(sb, field)
			}
		} else {
			for i, field := range q.col_keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				q.write_update_duplicate_field(sb, field)
			}
		}
		
		sb.WriteByte('\n')
	}
	//audit.Audit()
	return sb.String(), nil
}

func (q *Inserts_query) compile_inserts(sb *sbuilder){
	sb.WriteString("INSERT .")
	sb.WriteString(q.table)
	sb.WriteString(" (")
	for i, k := range q.col_keys {
		if i > 0 {
			sb.WriteString(",")
		}
		q.write_field(sb, k)
	}
	sb.WriteString(")\n")
}

func (q *Inserts_query) compile_fields(sb *sbuilder) error {
	length := len(q.fields)
	if length == 0 {
		return fmt.Errorf("No rows to insert")
	}
	
	q.alloc_data_capacity(q.col_count * length)
	
	for i := range q.fields {
		if i > 0 {
			sb.WriteByte(',')
		}
		
		sb.WriteByte('(')
		field_placeholder_list(q.col_count, sb)
		sb.WriteByte(')')
		
		q.data = append(q.data, q.fields[i]...)
	}
	return nil
}

func (q *Inserts_query) write_update_duplicate_field(sb *sbuilder, field string){
	q.write_field(sb, field)
	sb.WriteString("=VALUES(")
	q.write_field(sb, field)
	sb.WriteByte(')')
}