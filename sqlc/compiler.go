package sqlc

import "strings"

type compiler struct {
	sb			sbuilder
	use_alias	bool
	tables		map[string]string
	data		[]any
}

func (c *compiler) reset(){
	c.sb.Reset()
	c.use_alias = false
	if c.tables != nil {
		clear(c.tables)
	}
	c.data = c.data[:0]
}

func (c *compiler) write_field(t, field string){
	if !c.use_alias {
		c.sb.WriteString(field)
		return
	}
	if strings.IndexByte(field, '.') == -1 {
		c.sb.WriteString(t)
		c.sb.WriteByte('.')
	}
	c.sb.WriteString(field)
}

func (c *compiler) append_data(val any){
	if val == nil {
		return
	}
	//	Flatten data slices
	if v, ok := val.([]any); ok {
		length := len(v)
		if length == 0 {
			return
		}
		
		c.alloc_data_capacity(len(c.data) + length)
		
		c.data = append(c.data, v...)
	} else {
		c.data = append(c.data, val)
	}
}

func (c *compiler) alloc_data_capacity(total int){
	if cap(c.data) < total {
		new_data := make([]any, len(c.data), total)
		copy(new_data, c.data)
		c.data = new_data
	}
}