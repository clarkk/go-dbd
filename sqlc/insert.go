package sqlc

import (
	"fmt"
	"maps"
	"slices"
)

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
	q.fields = Fields()
	keys := slices.Sorted(maps.Keys(fields))
	for _, field := range keys {
		q.fields.Value(field, fields[field])
	}
	return q
}

func (q *Insert_query) Left_join(table, t, field, field_foreign string, conditions Map) *Insert_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Insert_query) Compile() (string, error){
	ctx := compiler_pool.Get().(*compiler)
	defer func() {
		ctx.reset()
		compiler_pool.Put(ctx)
	}()
	
	t := q.base_table_short()
	if err := q.compile_tables(ctx, t); err != nil {
		return "", err
	}
	
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
	ctx.sb.Alloc(alloc)
	//audit.Grow(alloc)
	ctx.sb.WriteString("INSERT .")
	ctx.sb.WriteString(q.table)
	ctx.sb.WriteByte('\n')
	ctx.sb.WriteString("SET ")
	if err := q.compile_fields(ctx); err != nil {
		return "", err
	}
	ctx.sb.WriteByte('\n')
	if q.update_duplicate {
		ctx.sb.WriteString("ON DUPLICATE KEY UPDATE ")
		err := q.compile_update_duplicate_fields(ctx)
		if err != nil {
			return "", err
		}
		ctx.sb.WriteByte('\n')
	}
	//audit.Audit()
	
	q.data_compiled = ctx.data
	return ctx.sb.String(), nil
}

func (q *Insert_query) compile_fields(ctx *compiler) error {
	length	:= len(q.fields.entries)
	ctx.alloc_data_capacity(len(ctx.data) + length)
	unique	:= make(map[string]struct{}, length)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		
		ctx.write_field(q.t, entry.field)
		ctx.sb.WriteString("=?")
		
		ctx.append_data(entry.value)
		unique[entry.field]			= struct{}{}
		q.map_fields[entry.field]	= i
	}
	return nil
}

func (q *Insert_query) compile_update_duplicate_fields(ctx *compiler) error {
	if q.update_duplicate_fields != nil {
		length := len(q.update_duplicate_fields)
		
		ctx.alloc_data_capacity(len(ctx.data) + length)
		
		for i, field := range q.update_duplicate_fields {
			j, found := q.map_fields[field]
			if !found {
				return fmt.Errorf("Invalid field: %s", field)
			}
			
			if i > 0 {
				ctx.sb.WriteString(", ")
			}
			
			q.write_update_field(ctx, field, q.fields.entries[j].operator)
			
			ctx.append_data(q.fields.entries[j].value)
		}
	} else {
		length := len(q.fields.entries)
		
		ctx.alloc_data_capacity(len(ctx.data) + length)
		
		for i, entry := range q.fields.entries {
			if i > 0 {
				ctx.sb.WriteString(", ")
			}
			
			q.write_update_field(ctx, entry.field, entry.operator)
			
			ctx.append_data(entry.value)
		}
	}
	return nil
}