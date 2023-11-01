package dbd

import (
	"fmt"
	"context"
	"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbt"
)

const (
	SQL_SELECT 	= "select"
	SQL_ORDER 	= "order"
	SQL_LIMIT 	= "limit"
)

var (
	reserved = map[string]bool{
		SQL_SELECT:	true,
		SQL_ORDER:	true,
		SQL_LIMIT:	true,
	}
)

type (
	Collection struct {
		list 		views
	}
	
	views 			map[string]dbt.View
)

func NewCollection() *Collection {
	return &Collection{
		list: views{},
	}
}

func (c *Collection) Add(view dbt.View) *Collection {
	table 	:= view.Table()
	name 	:= table.Name()
	
	//	Check if table is duplicated
	if _, ok := c.list[name]; ok {
		panic("Table is already added to collection: "+name)
	}
	
	//	Check for reserved keywords
	for k, _ := range table.Fields() {
		if _, ok := reserved[k]; ok {
			panic("Reserved keyword in: "+name+"."+k)
		}
	}
	
	c.list[name] = view
	return c
}

func (c *Collection) Get(ctx context.Context, name string) (*query_get, error) {
	view, ok := c.list[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Table invalid: %s", name))
	}
	
	return &query_get{
		query: query{
			view:	view,
		},
		ctx:	ctx,
	}, nil
}