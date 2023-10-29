package dbd

import (
	"fmt"
	"context"
	"github.com/clarkk/go-dbd/dbt"
)

type (
	list map[*dbt.Table]dbt.Collect
	
	Collection struct {
		list list
	}
)

func NewCollection() *Collection {
	return &Collection{
		list: list{},
	}
}

func (c *Collection) Add(table dbt.Collect) *Collection {
	if _, ok := c.list[table.Table]; ok {
		panic("Table is already added to collection: "+table.Table.Name)
	}
	
	c.list[table.Table] = table
	return c
}

func (c *Collection) Get(ctx context.Context, table *dbt.Table){
	target, ok := c.list[table]
	if !ok {
		panic("Table is not found in this collection: "+table.Name)
	}
	
	fmt.Println("get target: "+table.Name)
	fmt.Println(target)
	
	/*stmt, err := tx.PrepareContext(ctx, "SELECT id, timeout, lang FROM client WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()*/
}