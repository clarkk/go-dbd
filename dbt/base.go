package dbt

const (
	left_join 	= "LEFT JOIN"
	inner_join 	= "INNER JOIN"
)

type (
	field struct {
		tbl 	string
		col 	string
	}
	
	join struct {
		mode 	string
		col 	string
		foreign string
	}
	
	fields 		map[string]field
	joins 		map[string]join
	
	table struct {
		fields 	fields
		joins 	joins
	}
)

func (t *table) Select(){
	
}

func (t *table) Where(){
	
}