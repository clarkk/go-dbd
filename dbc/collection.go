package dbc

import (
	"fmt"
	"context"
	//"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbq"
	"github.com/clarkk/go-dbd/dbv"
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
		list 	dbv.Views
	}
)

func NewCollection() *Collection {
	return &Collection{
		list: dbv.Views{},
	}
}

func (c *Collection) Apply(view *dbv.View) *Collection {
	table 	:= view.Table()
	name 	:= table.Name()
	
	//	Check if table is duplicated
	if _, found := c.list[name]; found {
		panic("Table is already added to collection: "+name)
	}
	
	//	Check for reserved keywords in fields
	for _, k := range reserved {
		if table.Exists(k) {
			panic("Reserved keyword in: "+name+"."+k)
		}
	}
	
	c.list[name] = view
	return c
}

func (c *Collection) Get(ctx context.Context, name string) (*dbq.Query_get, error) {
	//	Check if table exists
	view, found := c.list[name]
	if !found {
		return nil, fmt.Errorf("Table invalid: %s", name)
	}
	return dbq.Get(ctx, name, view), nil
}