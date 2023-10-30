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

var reserved = map[string]bool{
	SQL_SELECT:	true,
	SQL_ORDER:	true,
	SQL_LIMIT:	true,
}

type (
	list map[string]dbt.View
	
	Collection struct {
		list list
	}
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

func (c *Collection) Get(ctx context.Context, name string) (*Get, error) {
	view, ok := c.list[name]
	if !ok {
		return nil, errors.New("Invalid table")
	}
	
	query := &Get{
		view: view,
	}
	
	fmt.Println("get target: "+view.Table().Name())
	//fmt.Println("pub:", g.view.Public())
	
	return query, nil
	
	/*stmt, err := tx.PrepareContext(ctx, "SELECT id, timeout, lang FROM client WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()*/
}