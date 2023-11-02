package dbv

import "github.com/clarkk/go-dbd/dbt"

type (
	View struct {
		table 	*dbt.Table
		public 	bool
	}
	
	Views 		map[string]View
)

func NewView(table *dbt.Table, public bool) View {
	return View{
		table,
		public,
	}
}

func (v View) Table() *dbt.Table {
	return v.table
}

func (v View) Public() bool {
	return v.public
}