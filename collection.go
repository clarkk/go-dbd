package dbd

import (
	"fmt"
	"context"
	"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbt"
)

type (
	list map[string]dbt.Collect
	
	Collection struct {
		list list
	}
)

func NewCollection() *Collection {
	return &Collection{
		list: list{},
	}
}

func (c *Collection) Add(collect dbt.Collect) *Collection {
	name := collect.Table().Name()
	if _, ok := c.list[name]; ok {
		panic("Table is already added to collection: "+name)
	}
	c.list[name] = collect
	return c
}

func (c *Collection) Get(ctx context.Context, name string) (*Get, error) {
	collect, ok := c.list[name]
	if !ok {
		return nil, errors.New("Invalid table")
	}
	
	g := &Get{}
	
	fmt.Println("get target: "+collect.Table().Name())
	
	return g, nil
	
	/*stmt, err := tx.PrepareContext(ctx, "SELECT id, timeout, lang FROM client WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()*/
}