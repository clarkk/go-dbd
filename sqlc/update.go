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

func (q *Update_query) Left_join(table, t, field, field_foreign string, conditions Map) *Update_query {
	q.left_join(table, t, field, field_foreign, conditions)
	return q
}

func (q *Update_query) Where(clause *Where_clause) *Update_query {
	q.where_clause = clause
	return q
}

func (q *Update_query) Compile() (string, []any, error){
	ctx := compiler_pool.Get().(*compiler)
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
	ctx.root_t = q.t
	
	//audit := Audit(sb, "update")
	
	//	Pre-allocation
	alloc := 14 + len(q.table) + alloc_field_assign(len(q.fields.entries))	//	"UPDATE .\n" + "SET \n"
	if ctx.use_alias {
		alloc += 2 + len(q.t)
	}
	ctx.sb.Alloc(alloc)
	//audit.Grow(alloc)
	
	ctx.sb.WriteString("UPDATE .")
	ctx.sb.WriteString(q.table)
	if ctx.use_alias {
		ctx.sb.WriteByte(' ')
		ctx.sb.WriteString(q.t)
		ctx.sb.WriteByte('\n')
		q.compile_joins(ctx, nil)
	} else {
		ctx.sb.WriteByte('\n')
	}
	ctx.sb.WriteString("SET ")
	if err := q.compile_fields(ctx); err != nil {
		return "", nil, err
	}
	ctx.sb.WriteByte('\n')
	//audit.Audit()
	if err := q.compile_where(ctx); err != nil {
		return "", nil, err
	}
	
	return ctx.sb.String(), ctx.data, nil
}

func (q *Update_query) compile_fields(ctx *compiler) error {
	length := len(q.fields.entries)
	ctx.alloc_data_capacity(len(ctx.data) + length)
	unique := make(map[string]struct{}, length)
	
	for i, entry := range q.fields.entries {
		if _, found := unique[entry.field]; found {
			return fmt.Errorf("Duplicate field: %s", entry.field)
		}
		if i > 0 {
			ctx.sb.WriteString(", ")
		}
		
		q.write_update_field(ctx, entry.field, entry.operator)
		
		ctx.append_data(entry.value)
		unique[entry.field]	= struct{}{}
	}
	return nil
}