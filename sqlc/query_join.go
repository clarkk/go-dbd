package sqlc

import (
	"fmt"
	"slices"
	"strings"
)

const (
	ROOT_ALIAS			= "<root>"
	root_alias_len		= len(ROOT_ALIAS)
	
	join_inner			= "JOIN"
	join_left			= "LEFT JOIN"
	
	char_table			= "abcdefghijklmnopqrstuvwxyz"
)

type (
	Join_conditions		[]Join_condition
	Join_condition struct {
		Field 			string
		Field_foreign 	string
		
		Fixed_value		bool
		Operator		Operator
		Field_value		any
	}
	
	query_join struct {
		query
		t 				string
		joined 			bool
		joined_t		bool		//	Joined on a non-base (pre-defined) table
		joins 			[]join
		optimize_joins	bool
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string		//	Table alias
		join_t			[]string	//	Join on a non-base (pre-defined) table (table alias)
		on				Join_conditions
		depth			int
	}
)

func (q *query_join) inner_join(table, t, field, field_foreign string){
	q.join(join_inner, table, t, field, field_foreign)
}

func (q *query_join) left_join(table, t, field, field_foreign string){
	q.join(join_left, table, t, field, field_foreign)
}

func (q *query_join) inner_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_fixed(join_inner, table, t, field, field_foreign, field_fixed, value_fixed)
}

func (q *query_join) left_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_fixed(join_left, table, t, field, field_foreign, field_fixed, value_fixed)
}

func (q *query_join) inner_join_multi(table, t string, fields Join_conditions){
	q.join_multi(join_inner, table, t, fields)
}

func (q *query_join) left_join_multi(table, t string, fields Join_conditions){
	q.join_multi(join_left, table, t, fields)
}

func (q *query_join) join(mode, table, t, field, field_foreign string){
	fields := Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	}}
	q.join_multi(mode, table, t, fields)
}

func (q *query_join) join_fixed(mode, table, t, field, field_foreign, field_fixed string, value_fixed any){
	fields := Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	},{
		Field:			field_fixed,
		Fixed_value:	true,
		Operator:		Op_eq,
		Field_value:	value_fixed,
	}}
	q.join_multi(mode, table, t, fields)
}

func (q *query_join) join_multi(mode, table, t string, fields Join_conditions){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			mode,
		table:			table,
		t:				t,
		join_t:			q.join_condition_foreign(fields),
		on:				fields,
	})
}

func (q *query_join) join_condition_foreign(fields Join_conditions) []string {
	join_t := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.Fixed_value {
			continue
		}
		
		// Join on a non-base (pre-defined) table
		if i := strings.IndexByte(f.Field_foreign, '.'); i != -1 {
			q.joined_t	= true
			join_t		= append(join_t, f.Field_foreign[:i])
		}
	}
	return join_t
}

func (q *query_join) resolve_alias_join_dependencies(list alias_collect) error {
	changed			:= true
	max_iterations	:= len(q.joins) + 1
	
	var iterations int
	for changed {
		changed = false
		iterations++
		
		if iterations > max_iterations {
			return fmt.Errorf("Circular dependency detected in joins")
		}
		
		for i := range q.joins {
			j := &q.joins[i]	//	Avoid copying data
			if _, ok := list[j.t]; ok {
				//	Check if joined on base table
				if len(j.join_t) == 0 {
					continue
				}
				
				var max_depth int
				for _, alias := range j.join_t {
					//	Collect dependency alias
					if _, exists := list[alias]; !exists {
						list[alias] = struct{}{}
						changed = true
					}
					
					for _, parent := range q.joins {
						if parent.t == alias {
							depth := parent.depth + 1
							if depth > max_depth {
								max_depth = depth
							}
							break
						}
					}
				}
				
				if j.depth != max_depth {
					j.depth = max_depth
					changed = true
				}
			}
		}
	}
	
	return nil
}

func (q *query_join) compile_tables(ctx *compiler, t string) error {
	if ctx.use_alias {
		//	Check for char collisions in joined tables
		for i := range q.joins {
			alias := q.joins[i].t
			if _, ok := ctx.tables[alias]; ok {
				return fmt.Errorf("Join table short already used: %s (%s)", alias, q.joins[i].table)
			}
			ctx.tables[alias] = q.joins[i].table
		}
	}
	
	//	Get available char for base table (a-z)
	if _, ok := ctx.tables[t]; ok {
		var found bool
		for i := range len(char_table) {
			char := char_table[i : i+1]
			if _, ok := ctx.tables[char]; !ok {
				t = char
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("No available table aliases for table: %s", q.table)
		}
	}
	
	q.t 			= t
	ctx.tables[t]	= q.table
	return nil
}

func (q *query_join) compile_from(ctx *compiler){
	ctx.sb.WriteString("FROM .")
	ctx.sb.WriteString(q.table)
	if ctx.use_alias {
		ctx.sb.WriteByte(' ')
		ctx.sb.WriteString(q.t)
	}
	ctx.sb.WriteByte('\n')
}

func (q *query_join) compile_joins(ctx *compiler, aliases alias_collect) error {
	if !q.joined {
		return nil
	}
	
	var joins_compile []join
	if q.optimize_joins {
		joins_compile = q.compile_optimize_joins(aliases)
	} else {
		joins_compile = q.joins
	}
	
	//	Pre-allocation
	ctx.sb.Alloc((20 + alloc_join_clause) * len(joins_compile))
	
	for i := range joins_compile {
		j := &joins_compile[i]	//	Avoid copying struct
		
		ctx.sb.WriteString(j.mode)
		ctx.sb.WriteString(" .")
		ctx.sb.WriteString(j.table)
		ctx.sb.WriteByte(' ')
		ctx.sb.WriteString(j.t)
		ctx.sb.WriteString(" ON ")
		
		for e, jf := range j.on {
			if e > 0 {
				ctx.sb.WriteString(" AND ")
			}
			ctx.sb.WriteString(j.t)
			ctx.sb.WriteByte('.')
			ctx.sb.WriteString(jf.Field)
			
			if jf.Fixed_value {
				sub_data, err := write_operator_condition(&ctx.sb, jf.Operator, jf.Field_value)
				if err != nil {
					return err
				}
				
				if jf.Operator == Op_null || jf.Operator == Op_not_null {
					continue
				}
				
				if sub_data != nil {
					ctx.append_data(sub_data)
				} else {
					ctx.append_data(jf.Field_value)
				}
			} else {
				ctx.sb.WriteByte('=')
				ctx.write_field(q.t, jf.Field_foreign)
			}
		}
		
		ctx.sb.WriteByte('\n')
	}
	return nil
}

func (q *query_join) compile_optimize_joins(aliases alias_collect) []join {
	joins_compile := aliases.filter(q.joins)
	
	if len(joins_compile) > 1 {
		//	Sort joins
		slices.SortFunc(joins_compile, func(a, b join) int {
			//	First priority: Depth
			if a.depth != b.depth {
				return a.depth - b.depth
			}
			//	Second priority: Inner join
			if a.mode != b.mode {
				if a.mode == join_inner {
					return -1
				}
				if b.mode == join_inner {
					return 1
				}
			}
			//	Sort alphabetically if same depth
			return strings.Compare(a.t, b.t)
		})
	}
	
	return joins_compile
}

func (q *query_join) write_update_field(ctx *compiler, field, operator string){
	switch operator {
	case op_update_add:
		ctx.write_field(q.t, field)
		ctx.sb.WriteByte('=')
		ctx.write_field(q.t, field)
		ctx.sb.WriteString("+?")
	default:
		ctx.write_field(q.t, field)
		ctx.sb.WriteString("=?")
	}
}

func (q *query_join) base_table_short() string {
	return q.table[:1]
}