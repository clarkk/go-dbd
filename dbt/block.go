package dbt

var Block = table{
	fields: fields{
		"id":			field{"block", "id"},
		"client_id":	field{"block", "client_id"},
		"is_suspended":	field{"client", "is_suspended"},
		"name":			field{"block", "name"},
	},
	joins: joins{
		"client":		join{left_join, "client_id", "id"},
	},
}