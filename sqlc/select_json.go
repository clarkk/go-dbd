package sqlc

import "strings"

type (
	json_table struct {
		t 			string
		json_doc	string
		json_path	string
		columns		[]json_column
	}
	
	json_column struct {
		property	string
		data_type	string
		path		string
	}
)

func JSON_table(t string) *json_table {
	return &json_table{
		t:			t,
		columns:	[]json_column{},
	}
}

func (j *json_table) Source(json_doc, json_path string) *json_table {
	j.json_doc	= json_doc
	j.json_path	= json_path
	return j
}

func (j *json_table) Source_key_value(json_doc, json_path string) *json_table {
	j.json_doc	= "JSON_KEY_VALUE("+json_doc+", '$')"
	j.json_path	= json_path
	return j
}

func (j *json_table) Column_path(property, data_type, path string) *json_table {
	j.columns = append(j.columns, json_column{
		property:	property,
		data_type:	data_type,
		path:		path,
	})
	return j
}

func (j *json_table) compile() string {
	s := ", JSON_TABLE(\n"
	s += "\t"+j.json_doc+", '"+j.json_path+"'\n"
	s += "\tCOLUMNS ("+j.compile_columns()+")\n"
	s += ") "+j.t
	return s
}

func (j *json_table) compile_columns() string {
	list := make([]string, len(j.columns))
	for i, c := range j.columns {
		list[i] = c.property+" "+c.data_type+" PATH '"+c.path+"'"
	}
	return strings.Join(list, ", ")
}