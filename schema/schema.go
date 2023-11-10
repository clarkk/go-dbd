package schema

import (
	"log"
	"context"
	"strings"
	"regexp"
	"strconv"
	"database/sql"
	"github.com/clarkk/go-util/cutil"
)

const (
	SCHEMA_CHAR 	= "char"
	SCHEMA_INT 		= "int"
	SCHEMA_DEC 		= "decimal"
)

var (
	schema 			= map[string]schemas{}
	
	integers = map[string]int{
		"tinyint":		int_pow(2, 8),
		"smallint":		int_pow(2, 16),
		"mediumint":	int_pow(2, 24),
		"int":			int_pow(2, 32),
		"bigint":		int_pow(2, 64),
	}
	
	schema_char 	= regexp.MustCompile(`^(varchar|char)\((\d+)\)`)
	schema_int 		= regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\((\d+)\)(?: (.*))?`)
	schema_decimal 	= regexp.MustCompile(`^(decimal)\((\d+),(\d+)\)(?: (.*))?`)
)

type (
	Schema struct {
		Type 		string
		Subtype 	string
		Length 		int
		Length_dec 	int
		Null 		bool
		Unsigned 	bool
		Range 		length_range
		Range_dec 	length_range_dec
	}
	
	length_range struct {
		Min 	int
		Max		int
	}
	
	length_range_dec struct {
		Min 	float64
		Max		float64
	}
	
	schemas 		map[string]Schema
)

func Fetch_schema(db *sql.DB){
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
		
		fetch_schema_table(db, table)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("Fetch DB schema table: "+err.Error())
	}
}

func Format(table string, column string, value []uint8) any {
	format, found := schema[table][column]
	if !found {
		panic("Could not lookup schema column: "+table+"."+column)
	}
	switch format.Type {
	case SCHEMA_CHAR:
		return string(value)
		
	case SCHEMA_INT:
		i, err := strconv.Atoi(string(value))
		if err != nil {
			panic("Invalid schema column int: "+table+"."+column+" "+err.Error())
		}
		return i
		
	case SCHEMA_DEC:
		if format.Length_dec == 0 {
			i, err := strconv.Atoi(string(value))
			if err != nil {
				panic("Invalid schema column int: "+table+"."+column+" "+err.Error())
			}
			return i
		}else{
			return string(value)
		}
		
	default:
		panic("Unsupported database data type: "+format.Type)
	}
}

func fetch_schema_table(db *sql.DB, table string){
	cutil.Out("."+table)
	
	schema[table] = schemas{}
	
	rows, err := db.QueryContext(context.Background(), "SHOW COLUMNS FROM ."+table)
	if err != nil {
		log.Fatal("Fetch DB schema columns: "+err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var (
			column 	string
			format 	string
			null 	string
			key 	string
			def 	any
			extra 	string
		)
		if err := rows.Scan(&column, &format, &null, &key, &def, &extra); err != nil {
			log.Fatal("Fetch DB schema columns: "+err.Error())
		}
		
		var (
			is_null			= valid_null(null)
			is_unsigned 	bool
		)
		
		matches := schema_int.FindStringSubmatch(format)
		if len(matches) != 0 {
			is_unsigned = valid_unsigned(matches[3])
			
			var (
				min int
				int_range = integers[matches[1]]
			)
			if !is_unsigned {
				min = int_range / -2
			}
			
			length, _ := strconv.Atoi(matches[2])
			schema[table][column] = Schema{
				Type:		SCHEMA_INT,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
				Unsigned:	is_unsigned,
				Range:		length_range{min, min + int_range - 1},
			}
			continue
		}
		
		matches = schema_char.FindStringSubmatch(format)
		if len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			schema[table][column] = Schema{
				Type:		SCHEMA_CHAR,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
			}
			continue
		}
		
		matches = schema_decimal.FindStringSubmatch(format)
		if len(matches) != 0 {
			is_unsigned = valid_unsigned(matches[4])
			
			length, _ 	:= strconv.Atoi(matches[2])
			dec, _ 		:= strconv.Atoi(matches[3])
			
			min, max 	:= decimal_range(length, dec, is_unsigned)
			
			schema[table][column] = Schema{
				Type:		SCHEMA_DEC,
				Subtype:	matches[1],
				Length:		length,
				Length_dec:	dec,
				Null:		is_null,
				Unsigned:	is_unsigned,
				Range_dec:	length_range_dec{min, max},
			}
			continue
		}
		
		log.Fatal("Unknown column: "+column+" "+format)
	}
	if err := rows.Err(); err != nil {
		log.Fatal("Fetch DB schema columns: "+err.Error())
	}
}

func valid_null(s string) bool {
	switch s {
	case "YES":
		return true
	case "NO":
	default:
		log.Fatal("Fetch DB schema columns invalid NULL")
	}
	return false
}

func valid_unsigned(s string) bool {
	switch s {
	case "unsigned":
		return true
	case "":
	default:
		log.Fatal("Fetch DB schema columns invalid unsigned")
	}
	return false
}

func decimal_range(length int, dec int, unsigned bool) (float64, float64) {
	l, _ := strconv.ParseFloat(strings.Repeat("9", length), 64)
	d, _ := strconv.ParseFloat("1"+strings.Repeat("0", dec), 64)
	
	var (
		min float64
		max = l / d
	)
	if !unsigned {
		min = max * -1
	}
	return min, max
}

func int_pow(n, m int) int {
	if m == 0 {
		return 1
	}
	result := n
	for i := 2; i <= m; i++ {
		result *= n
	}
	return result
}