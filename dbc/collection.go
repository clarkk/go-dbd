package dbc

import (
	"fmt"
	"context"
	"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbq"
	"github.com/clarkk/go-dbd/dbt"
)

const (
	SQL_SELECT 	= "select"
	SQL_ORDER 	= "order"
	SQL_LIMIT 	= "limit"
)

var (
	reserved = []string{
		SQL_SELECT,
		SQL_ORDER,
		SQL_LIMIT,
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
	for _, k := range reserved {
		if table.Exists(k) {
			panic("Reserved keyword in: "+name+"."+k)
		}
	}
	
	c.list[name] = view
	return c
}

func (c *Collection) Get(ctx context.Context, name string) (*dbq.Query_get, error) {
	view, ok := c.list[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Table invalid: %s", name))
	}
	
	return dbq.NewQuery_get(ctx, view), nil
}