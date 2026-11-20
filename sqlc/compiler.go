package sqlc

import "strings"

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

func (c *compiler) reset(){
	if c.sb.Cap() > 2*1024 {
		c.sb = sbuilder{}
	} else {
		c.sb.Reset()
	}
	
	c.use_alias	= false
	c.root_t	= ""
	
	clear(c.tables)
	
	clear(c.data)
	c.data = c.data[:0]
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
		c.data = append(make([]any, 0, total), c.data...)
	}
}