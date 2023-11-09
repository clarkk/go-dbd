package dbd

import (
	"log"
	"context"
	"regexp"
	"strconv"
	"github.com/clarkk/go-util/cutil"
)

var (
	schema_char 	= regexp.MustCompile(`^(varchar|char)\((\d+)\)`)
	schema_int 		= regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\((\d+)\)(?: (.*))?`)
	schema_decimal 	= regexp.MustCompile(`^(decimal)\((\d+),(\d+)\)(?: (.*))?`)
)

func fetch_schema(){
	cutil.Out("Fetching DB schema")
	
	rows, err := db.QueryContext(context.Background(), "SHOW TABLES")
	if err != nil {
		log.Fatal("Fetch DB schema: "+err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Fatal("Fetch DB schema table: "+err.Error())
		}
		
		fetch_schema_table(table)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("Fetch DB schema table: "+err.Error())
	}
}

func fetch_schema_table(table string){
	cutil.Out("."+table)
	
	schema[table] = schemas{}
	
	rows, err := db.QueryContext(context.Background(), "SHOW COLUMNS FROM ."+table)
	if err != nil {
		log.Fatal("Fetch DB schema columns: "+err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var (
			field 	string
			format 	string
			null 	string
			key 	string
			def 	any
			extra 	string
		)
		if err := rows.Scan(&field, &format, &null, &key, &def, &extra); err != nil {
			log.Fatal("Fetch DB schema columns: "+err.Error())
		}
		
		var (
			is_null			bool
			is_unsigned 	bool
		)
		switch null {
		case "YES":
			is_null = true
		case "NO":
		default:
			log.Fatal("Fetch DB schema columns invalid NULL")
		}
		
		matches := schema_char.FindStringSubmatch(format)
		if len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			schema[table][field] = Schema{
				Type:		SCHEMA_CHAR,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
			}
			continue
		}
		
		matches = schema_int.FindStringSubmatch(format)
		if len(matches) != 0 {
			switch matches[3] {
			case "unsigned":
				is_unsigned = true
			case "":
			default:
				log.Fatal("Fetch DB schema columns invalid unsigned")
			}
			
			var (
				min int
				int_range = integers[matches[1]]
			)
			if !is_unsigned {
				min = int_range / -2
			}
			
			length, _ := strconv.Atoi(matches[2])
			schema[table][field] = Schema{
				Type:		SCHEMA_INT,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
				Unsigned:	is_unsigned,
				Range:		length_range{
					Min:	min,
					Max:	min + int_range - 1,
				},
			}
			continue
		}
		
		matches = schema_decimal.FindStringSubmatch(format)
		if len(matches) != 0 {
			switch matches[4] {
			case "unsigned":
				is_unsigned = true
			case "":
			default:
				log.Fatal("Fetch DB schema columns invalid unsigned")
			}
			
			/*var (
				min int
			)
			if !is_unsigned {
				
			}*/
			
			length, _ 	:= strconv.Atoi(matches[2])
			dec, _ 		:= strconv.Atoi(matches[3])
			schema[table][field] = Schema{
				Type:		SCHEMA_DEC,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
				Unsigned:	is_unsigned,
				Length_dec:	dec,
				Range:		length_range{
					
				},
			}
			continue
		}
		
		log.Fatal("Unknown field: "+field+" "+format)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("Fetch DB schema columns: "+err.Error())
	}
}