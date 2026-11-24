package sqlc

import "fmt"

type query_where struct {
	query_join
	where_clause	*Where_clause
	use_id			bool
	id 				uint64
}

func (q *query_where) set_where(clause *Where_clause){
	if clause == nil {
		clause = &Where_clause{}
	}
	q.where_clause = clause
}

func (q *query_where) compile_where(c *compiler, inner_condition func(c *compiler, first *bool)) error {
	num, alloc, alloc_data := q.where_clause.get_alloc()
	
	if q.where_clause != nil && num == 0 {
		return fmt.Errorf("Empty WHERE clause")
	}
	
	if q.use_id {
		num++
		alloc += 4	//	"id=?"
		alloc_data++
	}
	
	if inner_condition != nil {
		num++
	}
	
	if num == 0 {
		return nil
	}
	
	//audit := Audit(sb, "where")
	
	//	Pre-allocation
	alloc += 7 + num * 5	//	"WHERE \n" + " AND "
	if c.use_alias {
		alloc += num * 3
	}
	
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	c.alloc_data_capacity(alloc_data)
	
	c.sb.WriteString("WHERE ")
	first := true
	
	if q.use_id {
		c.write_field(q.t, "id")
		c.sb.WriteString("=?")
		c.data = append(c.data, q.id)
		first = false
	}
	
	if inner_condition != nil {
		inner_condition(c, &first)
	}
	
	if q.where_clause != nil {
		writer := where_writer{
			q:			q,
			c:			c,
			has_term:	!first,
		}
		if err := writer.write_clause(q.where_clause); err != nil {
			return err
		}
	}
	c.sb.WriteByte('\n')
	//audit.Audit()
	return nil
}

func (q *query_where) write_condition_data(c *compiler, condition *where_condition) error {
	c.write_field(q.t, condition.field)
	return c.write_operator_condition(condition.operator, condition.value)
}