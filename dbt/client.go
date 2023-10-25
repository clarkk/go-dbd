package dbt

var Client = table{
	fields: fields{
		"id":			field{"client", "id"},
		"is_suspended":	field{"client", "is_suspended"},
		"time_created":	field{"client", "time_created"},
		"timeout":		field{"client", "timeout"},
		"lang":			field{"client", "lang"},
	},
}