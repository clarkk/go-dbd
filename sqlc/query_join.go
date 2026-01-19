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
	Join_conditions		[]join_condition
	
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
		conditions		Map
		depth			int
	}
	
	join_condition struct {
		field 			string
		field_foreign 	string
	}
)

func (q *query_join) inner_join(table, t, field, field_foreign string, conditions Map){
	q.join(join_inner, table, t, field, field_foreign, conditions)
}

func (q *query_join) left_join(table, t, field, field_foreign string, conditions Map){
	q.join(join_left, table, t, field, field_foreign, conditions)
}

func (q *query_join) inner_join_multi(table, t string, fields Join_conditions, conditions Map){
	q.join_multi(join_inner, table, t, fields, conditions)
}

func (q *query_join) left_join_multi(table, t string, fields Join_conditions, conditions Map){
	q.join_multi(join_left, table, t, fields, conditions)
}

func (q *query_join) join(mode, table, t, field, field_foreign string, conditions Map){
	fields := Join_conditions{{
		field:			field,
		field_foreign:	field_foreign,
	}}
	
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			mode,
		table:			table,
		t:				t,
		join_t:			q.join_condition_foreign(fields),
		on:				fields,
		conditions:		conditions,
	})
}

func (q *query_join) join_multi(mode, table, t string, fields Join_conditions, conditions Map){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:			mode,
		table:			table,
		t:				t,
		join_t:			q.join_condition_foreign(fields),
		on:				fields,
		conditions:		conditions,
	})
}

func (q *query_join) join_condition_foreign(fields Join_conditions) []string {
	join_t := make([]string, 0, len(fields))
	for _, f := range fields {
		// Join on a non-base (pre-defined) table
		if i := strings.IndexByte(f.field_foreign, '.'); i != -1 {
			q.joined_t	= true
			join_t		= append(join_t, f.field_foreign[:i])
		}
	}
	return join_t
}

func (q *query_join) resolve_alias_join_dependencies(list alias_collect){
	changed := true
	for changed {
		changed = false
		for i := range q.joins {
			j := &q.joins[i]	//	Avoid copying data
			if _, ok := list[j.t]; ok {
				//	Check if joined on non-base table
				if len(j.join_t) == 0 {
					continue
				}
				
				/*for _, depAlias := range j.join_t {
					if _, exists := list[depAlias]; !exists {
						list[depAlias] = struct{}{}
						changed = true
					}
				}
				
				maxDepth := 0
				for _, depAlias := range j.join_t {
					for _, parent := range q.joins {
						if parent.t == depAlias {
							if parent.depth+1 > maxDepth {
								maxDepth = parent.depth + 1
							}
							break
						}
					}
				}
				
				if j.depth != maxDepth {
					j.depth = maxDepth
					changed = true
				}*/
				
				if _, exists := list[j.join_t[0]]; !exists {
					list[j.join_t[0]] = struct{}{}
					changed = true
				}
				
				//	Find depth
				for _, parent := range q.joins {
					if parent.t == j.join_t[0] {
						depth := parent.depth + 1
						if j.depth != depth {
							j.depth = depth
							changed = true
						}
						break
					}
				}
			}
		}
	}
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

func (q *query_join) compile_joins(ctx *compiler, aliases alias_collect){
	if !q.joined {
		return
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
			ctx.sb.WriteString(jf.field)
			ctx.sb.WriteByte('=')
			ctx.write_field(q.t, jf.field_foreign)
		}
		
		if len(j.conditions) > 0 {
			//	Sort keys
			keys := make([]string, len(j.conditions))
			var i int
			for k := range j.conditions {
				keys[i] = k
				i++
			}
			slices.Sort(keys)
			
			for _, column := range keys {
				ctx.sb.WriteString(" AND ")
				ctx.sb.WriteString(j.t)
				ctx.sb.WriteByte('.')
				ctx.sb.WriteString(column)
				ctx.sb.WriteString("=?")
				
				ctx.append_data(j.conditions[column])
			}
		}
		ctx.sb.WriteByte('\n')
	}
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