package dbd

import (
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
	ERR_TABLE 		= errors.New("Table invalid")
	ERR_PRIVATE 	= errors.New("Table private")
	
	reserved = map[string]bool{
		SQL_SELECT:	true,
		SQL_ORDER:	true,
		SQL_LIMIT:	true,
	}
)

type (
	Collection struct {
		list list
	}
	
	list 			map[string]dbt.View
	
	select_field struct {
		fn 			string
		field 		string
		as 			string
	}
	
	select_clause 	[]select_field
)

func NewCollection() *Collection {
	return &Collection{
		list: list{},
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
	for _, k := range table.Fields() {
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
		return nil, ERR_TABLE
	}
	
	return &query_get{
		query: query{
			
		},
		ctx:	ctx,
		view:	view,
	}, nil
}