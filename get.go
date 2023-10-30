package dbd

import (
	"fmt"
	"github.com/clarkk/go-dbd/dbt"
)

type Get struct {
	view 	dbt.View
}

func (g *Get) Select(fields Select) *Get {
	fmt.Println("select:", fields)
	return g
}

func (g *Get) Where(fields Where) *Get {
	fmt.Println("where:", fields)
	return g
}

func (g *Get) Result(){
	fmt.Println(g.view.Table())
	
	/*stmt, err := tx.PrepareContext(ctx, "SELECT id, timeout, lang FROM client WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	
	rows, err := stmt.QueryContext(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	
	cols, _ := rows.Columns()
	cols_len := len(cols)
	for rows.Next() {
		columns 	:= make([]interface{}, cols_len)
		columns_ref := make([]interface{}, cols_len)
		for i, _ := range columns {
			columns_ref[i] = &columns[i]
		}
		
		if err := rows.Scan(columns_ref...); err != nil {
			log.Fatal(err)
		}
		
		m := make(map[string]interface{})
		for i, col_name := range cols {
			val := columns_ref[i].(*interface{})
			m[col_name] = *val
		}
		
		fmt.Println(m)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}*/
}