package sqlc

import "strings"

type (
	query_where struct {
		query_join
		conditions
		or_groups	[]*or_group
		use_id		bool
		id 			uint64
	}
	
	or_group struct {
		conditions
	}
	
	conditions []query_where_condition
	query_where_condition struct {
		clause		where_clause
		value		any
	}
)

func (q *conditions) where_clause(clause where_clause, value any) {
	*q = append(*q, query_where_condition{clause, value})
}

func (q *query_where) where_or_group() *or_group {
	g := &or_group{}
	q.or_groups = append(q.or_groups, g)
	return g
}

func (q *query_where) compile_where(sb *strings.Builder) error {
	length := q.count_conditions()
	
	if q.use_id {
		length++
	}
	if length == 0 {
		return nil
	}
	
	//	Pre-allocation
	sb.Grow(7 + length * alloc_where_condition)
	q.alloc_data_capacity(length + len(q.data))
	
	sb.WriteString("WHERE ")
	first := true
	
	if q.use_id {
		q.write_field(sb, "id")
		sb.WriteString("=?")
		q.data = append(q.data, q.id)
		first = false
	}
	
	//	Apply "or groups"
	if q.or_groups != nil {
		for _, group := range q.or_groups {
			if first {
				first = false
			} else {
				sb.WriteString(" AND ")
			}
			
			sb.WriteByte('(')
			for i, condition := range group.conditions {
				if i > 0 {
					sb.WriteString(" OR ")
				}
				q.write_field(sb, condition.clause.field)
				sb.WriteString(condition.clause.sql)
				
				q.append_data(condition.value)
			}
			sb.WriteByte(')')
		}
	}
	
	var duplicates map[string]string
	//	Only allocate if at least 2 conditions
	if len(q.conditions) > 1 {
		//	Pre-allocation
		duplicates = make(map[string]string, 2)
	}
	
	for _, condition := range q.conditions {
		if duplicates != nil {
			if operator, ok := duplicates[condition.clause.field]; ok {
				if err := check_operator_compatibility(operator, condition.clause.operator, condition.clause.field); err != nil {
					return err
				}
			} else {
				duplicates[condition.clause.field] = condition.clause.operator
			}
		}
		
		if first {
			first = false
		} else {
			sb.WriteString(" AND ")
		}
		
		if condition.clause.subquery != nil {
			sql_subquery, err := condition.clause.subquery.Compile()
			if err != nil {
				return err
			}
			condition.clause.sql = strings.Replace(condition.clause.sql, "?", sql_subquery, 1)
		}
		
		q.write_field(sb, condition.clause.field)
		sb.WriteString(condition.clause.sql)
		
		if condition.clause.operator == op_null || condition.clause.operator == op_not_null {
			continue
		}
		
		//	Apply data
		if condition.clause.subquery != nil {
			q.append_data(condition.clause.subquery.Data())
		} else {
			q.append_data(condition.value)
		}
	}
	sb.WriteByte('\n')
	return nil
}

func (q *query_where) count_conditions() int {
	n := len(q.conditions)
	for _, group := range q.or_groups {
		if group != nil {
			n += len(group.conditions)
		}
	}
	return n
}