package dbt

const (
	LEFT_JOIN 	join_mode = "LEFT JOIN"
	INNER_JOIN 	join_mode = "INNER JOIN"
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
		Name 	string
		Fields 	Fields
		Joins 	Joins
	}
	
	Collect struct {
		External 	bool
		Table 		*Table
		As 			string
	}
	
	join_mode 	string
)