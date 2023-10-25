package dbt

const (
	LEFT_JOIN 	= "LEFT JOIN"
	INNER_JOIN 	= "INNER JOIN"
)

type (
	Field struct {
		Tbl 	string
		Col 	string
	}
	
	Join struct {
		Mode 	string
		Col 	string
		Foreign string
	}
	
	Fields 		map[string]Field
	Joins 		map[string]Join
	
	Table struct {
		Fields 	Fields
		Joins 	Joins
	}
	
	Table_map struct {
		External 	bool
		Table 		Table
		Name 		string
	}
)