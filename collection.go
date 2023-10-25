package dbd

import "github.com/clarkk/go-dbd/dbt"

type (
	Collection struct {
		list map[*dbt.Table]dbt.Collect
	}
)

func (c *Collection) Add(table dbt.Collect){
	if _, ok := c.list[table.Table]; ok {
		panic("Table is already added to collection: "+table.Table.Name)
	}
	
	c.list[table.Table] = table
}