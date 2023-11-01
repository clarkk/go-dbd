package dbt

import (
	"slices"
)

const (
	LEFT_JOIN join_mode 	= "LEFT JOIN"
	INNER_JOIN join_mode 	= "INNER JOIN"
)

type (
	Field struct {
		Table 	string
		Col 	string
	}
	
	Join struct {
		Mode 	join_mode
		Col 	string
		Foreign string
	}
	
	Fields 		map[string]Field
	Joins 		map[string]Join
	
	Get 		[]string
	Put 		map[string]string
	
	Table struct {
		name 	string
		fields 	Fields
		joins 	Joins
		get 	Get
		put 	Put
	}
	
	View struct {
		table 	*Table
		as 		string
		public 	bool
	}
	
	join_mode 	string
)

func NewTable(name string, fields Fields, joins Joins, get Get, put Put) *Table {
	return &Table{
		name,
		fields,
		joins,
		get,
		put,
	}
}

func NewView(table *Table, as string, public bool) View {
	return View{
		table,
		as,
		public,
	}
}

func (t *Table) Exists(field string) bool {
	if _, found := t.fields[field]; found {
		return true
	}
	return false
}

func (t *Table) Exists_public(field string) bool {
	return slices.Contains(t.get, field)
}

func (t *Table) Joined(field string) bool {
	return t.name != t.fields[field].Table
}

func (t *Table) Col(field string) string {
	return t.fields[field].Col
}

func (t *Table) Name() string {
	return t.name
}

func (v View) Table() *Table {
	return v.table
}

func (v View) As() string {
	return v.as
}

func (v View) Public() bool {
	return v.public
}