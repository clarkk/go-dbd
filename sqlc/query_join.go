package sqlc

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

var char_table = [26]string{"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"}

type query_join struct {
	query
	t 			string
	joined 		bool
	joined_t	bool	//	Joined on a non-base (pre-defined) table
	joins 		[]join
}

func (q *query_join) left_join(table, t, field, field_foreign string, conditions Map){
	var join_t string
	// Join on a non-base (pre-defined) table
	if i := strings.IndexByte(field_foreign, '.'); i != -1 {
		q.joined_t	= true
		join_t		= field_foreign[:i]
	}
	
	q.joined = true
	
	q.joins = append(q.joins, join{
		mode:			"LEFT JOIN",
		table:			table,
		t:				t,
		join_t:			join_t,
		field:			field,
		field_foreign:	field_foreign,
		conditions:		conditions,
	})
}

func (q *query_join) compile_tables(ctx *compiler, t string) error {
	if ctx.use_alias {
		//	Check for char collisions in joined tables
		for i := range q.joins {
			j := &q.joins[i]	//	Avoid copying struct
			if _, ok := ctx.tables[j.t]; ok {
				return fmt.Errorf("Join table short already used: %s (%s)", j.t, j.table)
			}
			ctx.tables[j.t] = j.table
		}
	}
	
	//	Get available char for base table (a-z)
	if _, ok := ctx.tables[t]; ok {
		var found bool
		for i := range 26 {
			char := char_table[i]
			if _, ok := ctx.tables[char]; !ok {
				t 		= char
				found	= true
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

func (q *query_join) compile_joins(ctx *compiler){
	if !q.joined {
		return
	}
	
	if q.joined_t {
		//	Sort joins and put joins which does not join on the base table last
		slices.SortFunc(q.joins, func(a, b join) int {
			if a.join_t == "" && b.join_t != "" {
				return -1
			}
			if a.join_t != "" && b.join_t == "" {
				return 1
			}
			return 0
		})
	}
	
	//	Pre-allocation
	ctx.sb.Alloc((20 + alloc_join_clause) * len(q.joins))
	
	for i := range q.joins {
		j := &q.joins[i]	//	Avoid copying struct
		ctx.sb.WriteString(j.mode)
		ctx.sb.WriteString(" .")
		ctx.sb.WriteString(j.table)
		ctx.sb.WriteByte(' ')
		ctx.sb.WriteString(j.t)
		ctx.sb.WriteString(" ON ")
		ctx.sb.WriteString(j.t)
		ctx.sb.WriteByte('.')
		ctx.sb.WriteString(j.field)
		ctx.sb.WriteByte('=')
		ctx.write_field(q.t, j.field_foreign)
		
		if len(j.conditions) > 0 {
			keys := slices.Sorted(maps.Keys(j.conditions))
			
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