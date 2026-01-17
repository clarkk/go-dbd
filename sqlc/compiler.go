package sqlc

import "strings"

type compiler struct {
	sb			sbuilder
	use_alias	bool
	root_t 		string
	tables		map[string]string
	data		[]any
}

func (c *compiler) reset(){
	c.sb.Reset()
	c.use_alias	= false
	c.root_t	= ""
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
	
	if pos := strings.IndexByte(field, '.'); pos != -1 {
		if pos == root_alias_len && field[:root_alias_len] == ROOT_ALIAS {
			c.sb.WriteString(c.root_t)
			c.sb.WriteString(field[pos:])
			return
		}
		c.sb.WriteString(field)
		return
	}
	
	c.sb.WriteString(t)
	c.sb.WriteByte('.')
	c.sb.WriteString(field)
}

func (c *compiler) append_data(val any){
	//	Flatten data slices
	switch v := val.(type) {
	case []any:
		length := len(v)
		if length == 0 {
			return
		}
		c.alloc_data_capacity(len(c.data) + length)
		c.data = append(c.data, v...)
		
	default:
		c.data = append(c.data, v)
	}
}

func (c *compiler) alloc_data_capacity(total int){
	if cap(c.data) < total {
		new_data := make([]any, len(c.data), total)
		copy(new_data, c.data)
		c.data = new_data
	}
}