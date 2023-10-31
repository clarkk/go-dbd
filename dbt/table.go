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

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Fields() Fields {
	return t.fields
}

func (t *Table) Joins() Joins {
	return t.joins
}

func (t *Table) Get() Get {
	return t.get
}

func (t *Table) Put() Put {
	return t.put
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