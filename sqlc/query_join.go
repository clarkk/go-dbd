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
	tables 		map[string]string
	joined 		bool
	joined_t	bool
	joins 		[]join
}

func (q *query_join) left_join(table, t, field, field_foreign string, conditions Map){
	var join_t string
	if before, _, found := strings.Cut(field_foreign, "."); found {
		q.joined_t	= true
		join_t		= before
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

func (q *query_join) compile_tables(c string) error {
	//	Reset
	q.data = q.data[:0]
	if q.tables == nil {
		q.tables = make(map[string]string, len(q.joins)+1)
	} else {
		clear(q.tables)
	}
	
	if q.joined {
		//	Check for char collisions in joined tables
		for i := range q.joins {
			j := &q.joins[i]	//	Avoid copying struct
			if _, ok := q.tables[j.t]; ok {
				return fmt.Errorf("Join table short already used: %s (%s)", j.t, j.table)
			}
			q.tables[j.t] = j.table
		}
		//	Get available char for base table (a-z)
		if _, ok := q.tables[c]; ok {
			for i := range 26 {
				char := char_table[i]
				if _, ok := q.tables[char]; !ok {
					c = char
					break
				}
			}
		}
	}
	q.t 		= c
	q.tables[c]	= q.table
	return nil
}

func (q *query_join) compile_from(sb *strings.Builder){
	sb.WriteString("FROM .")
	sb.WriteString(q.table)
	if q.joined {
		sb.WriteByte(' ')
		sb.WriteString(q.t)
	}
	sb.WriteByte('\n')
}

func (q *query_join) compile_joins(sb *strings.Builder){
	if !q.joined {
		return
	}
	
	if q.joined_t {
		//	Sort joins and put joins which doesn't join on the base table last
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
	sb.Grow((20 + alloc_join_clause) * len(q.joins))
	
	for i := range q.joins {
		j := &q.joins[i]	//	Avoid copying struct
		sb.WriteString(j.mode)
		sb.WriteString(" .")
		sb.WriteString(j.table)
		sb.WriteByte(' ')
		sb.WriteString(j.t)
		sb.WriteString(" ON ")
		sb.WriteString(j.t)
		sb.WriteByte('.')
		sb.WriteString(j.field)
		sb.WriteByte('=')
		q.write_field(sb, j.field_foreign)
		
		if len(j.conditions) > 0 {
			keys := slices.Sorted(maps.Keys(j.conditions))
			
			for _, column := range keys {
				sb.WriteString(" AND ")
				sb.WriteString(j.t)
				sb.WriteByte('.')
				sb.WriteString(column)
				sb.WriteString("=?")
				
				q.append_data(j.conditions[column])
			}
		}
		sb.WriteByte('\n')
	}
}

func (q *query_join) write_update_field(sb *strings.Builder, field, operator string){
	switch operator {
	case op_update_add:
		q.write_field(sb, field)
		sb.WriteByte('=')
		q.write_field(sb, field)
		sb.WriteString("+?")
	default:
		q.write_field(sb, field)
		sb.WriteString("=?")
	}
}

func (q *query_join) write_field(sb *strings.Builder, field string){
	if !q.joined {
		sb.WriteString(field)
		return
	}
	if strings.IndexByte(field, '.') == -1 {
		sb.WriteString(q.t)
		sb.WriteByte('.')
	}
	sb.WriteString(field)
}

func (q *query_join) base_table_short() string {
	return q.table[:1]
}