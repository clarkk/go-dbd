package sqlc

import (
	"fmt"
	"sync"
	"strings"
)

var compiler_pool = sync.Pool{
	New: func() any {
		//	Pre-allocation
		return &compiler{
			tables:	make(map[string]string, 5),
			data:	make([]any, 0, 5),
		}
	},
}

type compiler struct {
	sb			sbuilder
	use_alias	bool
	root_t 		string
	tables		map[string]string
	data		[]any
}

func (c *compiler) data_copy() []any {
	return append([]any(nil), c.data...)
}

func (c *compiler) write_field(t, field string){
	if !c.use_alias {
		c.sb.WriteString(field)
		return
	}
	
	pos := strings.IndexByte(field, '.')
	
	//	Base table
	if pos == -1 {
		c.sb.WriteString(t)
		c.sb.WriteByte('.')
		c.sb.WriteString(field)
		return
	}
	
	//	Root table
	if pos == root_alias_len && field[:root_alias_len] == ROOT_ALIAS {
		c.sb.WriteString(c.root_t)
		c.sb.WriteString(field[pos:])
		return
	}
	
	//	Joined table
	c.sb.WriteString(field)
}

func (c *compiler) write_operator_condition(operator Operator, value any) error {
	switch operator {
	case Op_null:
		c.sb.WriteString(" IS NULL")
		
	case Op_not_null:
		c.sb.WriteString(" IS NOT NULL")
		
	case Op_bt, Op_not_bt:
		values, ok := value.([]any)
		length := len(values)
		if !ok || length != 2 {
			return fmt.Errorf("BETWEEN requires exactly two values")
		}
		
		if operator == Op_not_bt {
			c.sb.WriteString(" NOT")
		}
		c.sb.WriteByte(' ')
		c.sb.WriteString(sql_op_bt)
		c.alloc_data_capacity(length)
		c.data = append(c.data, values...)
		
	case Op_in, Op_not_in:
		values, ok := value.([]any)
		length := len(values)
		if !ok || length == 0 {
			return fmt.Errorf("IN requires a non-empty []any")
		}
		
		if operator == Op_not_in {
			c.sb.WriteString(" NOT")
		}
		c.sb.WriteString(" IN (")
		c.write_field_placeholder_list(length)
		c.sb.WriteByte(')')
		c.alloc_data_capacity(length)
		c.data = append(c.data, values...)
		
	case Op_in_subquery:
		query, ok := value.(*Select_query)
		if !ok || query == nil {
			return fmt.Errorf("IN subquery requires a non-nil *Select_query")
		}
		
		sql, args, err := query.Compile()
		if err != nil {
			return err
		}
		c.sb.WriteString(" IN (\n")
		c.sb.WriteString(sql)
		c.sb.WriteByte(')')
		c.alloc_data_capacity(len(args))
		c.data = append(c.data, args...)
		
	case Op_eq, Op_not_eq, Op_gt, Op_gteq, Op_lt, Op_lteq:
		if _, ok := value.([]any); ok {
			return fmt.Errorf("Operator %s requires a single value", sql_ops[operator])
		}
		
		c.sb.WriteString(sql_ops[operator])
		c.sb.WriteByte('?')
		c.data = append(c.data, value)
		
	default:
		return fmt.Errorf("Unknown operator %d", operator)
	}
	
	return nil
}

func (c *compiler) write_field_placeholder_list(count int){
	if count == 0 {
		return
	}
	c.sb.WriteByte('?')
	for i := 1; i < count; i++ {
		c.sb.WriteString(",?")
	}
}

func (c *compiler) alloc_data_capacity(add int){
	total := len(c.data) + add
	if cap(c.data) < total {
		c.data = append(make([]any, 0, total), c.data...)
	}
}

func (c *compiler) release(){
	c.sb.Reset()
	c.use_alias	= false
	c.root_t	= ""
	clear(c.tables)
	clear(c.data)
	c.data = c.data[:0]
	
	compiler_pool.Put(c)
}