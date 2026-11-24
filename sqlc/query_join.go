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
	join_cross			= "CROSS JOIN"
	
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
		t 				string		//	Base table alias
		joined 			bool
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
		force			bool
	}
)

func (q *query_join) inner_join(table, t, field, field_foreign string){
	q.join(join_inner, table, t, field, field_foreign)
}

func (q *query_join) inner_join_force(table, t, field, field_foreign string){
	q.join_force(join_inner, table, t, field, field_foreign)
}

func (q *query_join) left_join(table, t, field, field_foreign string){
	q.join(join_left, table, t, field, field_foreign)
}

func (q *query_join) cross_join(table, t string){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:		join_cross,
		table:		table,
		t:			t,
	})
}

func (q *query_join) inner_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_fixed(join_inner, table, t, field, field_foreign, field_fixed, value_fixed)
}

func (q *query_join) inner_join_fixed_force(table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_fixed_force(join_inner, table, t, field, field_foreign, field_fixed, value_fixed)
}

func (q *query_join) left_join_fixed(table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_fixed(join_left, table, t, field, field_foreign, field_fixed, value_fixed)
}

func (q *query_join) inner_join_multi(table, t string, conditions Join_conditions){
	q.join_multi(join_inner, table, t, conditions)
}

func (q *query_join) left_join_multi(table, t string, conditions Join_conditions){
	q.join_multi(join_left, table, t, conditions)
}

func (q *query_join) join(mode, table, t, field, field_foreign string){
	q.join_multi(mode, table, t, Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	}})
}

func (q *query_join) join_force(mode, table, t, field, field_foreign string){
	q.join_multi_force(mode, table, t, Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	}})
}

func (q *query_join) join_fixed(mode, table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_multi(mode, table, t, Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	},{
		Field:			field_fixed,
		Fixed_value:	true,
		Operator:		Op_eq,
		Field_value:	value_fixed,
	}})
}

func (q *query_join) join_fixed_force(mode, table, t, field, field_foreign, field_fixed string, value_fixed any){
	q.join_multi_force(mode, table, t, Join_conditions{{
		Field:			field,
		Field_foreign:	field_foreign,
	},{
		Field:			field_fixed,
		Fixed_value:	true,
		Operator:		Op_eq,
		Field_value:	value_fixed,
	}})
}

func (q *query_join) join_multi(mode, table, t string, conditions Join_conditions){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:		mode,
		table:		table,
		t:			t,
		join_t:		conditions.foreign_aliases(),
		on:			conditions,
	})
}

func (q *query_join) join_multi_force(mode, table, t string, conditions Join_conditions){
	q.joined = true
	q.joins = append(q.joins, join{
		mode:		mode,
		table:		table,
		t:			t,
		join_t:		conditions.foreign_aliases(),
		on:			conditions,
		force:		true,
	})
}

func (j Join_conditions) foreign_aliases() []string {
	var join_t []string
	for _, c := range j {
		if c.Fixed_value {
			continue
		}
		
		//	Join on a non-base (pre-defined) table
		if i := strings.IndexByte(c.Field_foreign, '.'); i != -1 {
			alias := c.Field_foreign[:i]
			if alias != ROOT_ALIAS {
				join_t = append(join_t, alias)
			}
		}
	}
	return join_t
}

func (q *query_join) resolve_alias_join_dependencies(list alias_collect) error {
	join_map := make(map[string]*join, len(q.joins))
	for i := range q.joins {
		j := &q.joins[i]
		join_map[j.t] = j
		
		if j.force || j.mode == join_cross {
			list[j.t] = struct{}{}
		}
	}
	
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
			
			//	Check if joined on base table
			if _, ok := list[j.t]; !ok {
				continue
			}
			
			var max_depth int
			for _, alias := range j.join_t {
				//	Collect dependency alias
				if _, exists := list[alias]; !exists {
					list[alias] = struct{}{}
					changed = true
				}
				
				if parent := join_map[alias]; parent != nil {
					depth := parent.depth + 1
					if depth > max_depth {
						max_depth = depth
					}
				}
			}
			
			if j.depth != max_depth {
				j.depth = max_depth
				changed = true
			}
		}
	}
	
	return nil
}

func (q *query_join) compile_tables(c *compiler, t string) error {
	if c.use_alias {
		//	Check for char collisions in joined tables
		for i := range q.joins {
			j := &q.joins[i]
			
			if j.mode != join_cross && len(j.on) == 0 {
				return fmt.Errorf("%s %s requires at least one ON condition", j.mode, j.table)
			}
			
			if _, ok := c.tables[j.t]; ok {
				return fmt.Errorf("Join table short already used: %s (%s)", j.t, j.table)
			}
			c.tables[j.t] = j.table
		}
	}
	
	//	Get available char for base table (a-z)
	if _, ok := c.tables[t]; ok {
		var found bool
		for i := range len(char_table) {
			char := char_table[i : i+1]
			if _, ok := c.tables[char]; !ok {
				t = char
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("No available table aliases for table: %s", q.table)
		}
	}
	
	q.t 		= t
	c.tables[t]	= q.table
	return nil
}

func (q *query_join) compile_from(c *compiler){
	c.sb.WriteString("FROM .")
	c.sb.WriteString(q.table)
	if c.use_alias {
		c.sb.WriteByte(' ')
		c.sb.WriteString(q.t)
	}
	c.sb.WriteByte('\n')
}

func (q *query_join) compile_joins(c *compiler, aliases alias_collect) error {
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
	c.sb.Alloc((20 + alloc_join_clause) * len(joins_compile))
	
	for i := range joins_compile {
		j := &joins_compile[i]	//	Avoid copying struct
		
		c.sb.WriteString(j.mode)
		c.sb.WriteString(" .")
		c.sb.WriteString(j.table)
		c.sb.WriteByte(' ')
		c.sb.WriteString(j.t)
		
		if j.mode == join_cross {
			c.sb.WriteByte('\n')
			continue
		}
		
		c.sb.WriteString(" ON ")
		
		for e, jf := range j.on {
			if e > 0 {
				c.sb.WriteString(" AND ")
			}
			c.sb.WriteString(j.t)
			c.sb.WriteByte('.')
			c.sb.WriteString(jf.Field)
			
			if jf.Fixed_value {
				if err := c.write_operator_condition(jf.Operator, jf.Field_value); err != nil {
					return err
				}
			} else {
				c.sb.WriteByte('=')
				c.write_field(q.t, jf.Field_foreign)
			}
		}
		
		c.sb.WriteByte('\n')
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

func (q *query_join) write_update_field(c *compiler, field, operator string, value any){
	switch operator {
	case op_update_add:
		c.write_field(q.t, field)
		c.sb.WriteByte('=')
		c.write_field(q.t, field)
		c.sb.WriteString("+?")
	default:
		c.write_field(q.t, field)
		c.sb.WriteString("=?")
	}
	c.data = append(c.data, value)
}

func (q *query_join) base_table_short() string {
	return q.table[:1]
}