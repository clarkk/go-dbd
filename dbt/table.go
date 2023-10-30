package dbt

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
	
	Table struct {
		name 	string
		fields 	Fields
		joins 	Joins
	}
	
	Collect struct {
		table 	*Table
		as 		string
		public 	bool
	}
	
	join_mode 	string
)

func NewTable(name string, fields Fields, joins Joins) *Table {
	return &Table{
		name,
		fields,
		joins,
	}
}

func NewCollect(table *Table, as string, public bool) Collect {
	return Collect{
		table,
		as,
		public,
	}
}

func (t *Table) Name() string {
	return t.name
}

func (c Collect) Table() *Table {
	return c.table
}