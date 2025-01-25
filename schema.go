package dbd

import (
	"log"
	"context"
	"regexp"
	"strconv"
	"strings"
)

const (
	SCHEMA_CHAR 	= "char"
	SCHEMA_INT 		= "int"
	SCHEMA_DEC 		= "decimal"
	
	TYPE_TINYINT 	= "tinyint"
	TYPE_SMALLINT	= "smallint"
	TYPE_MEDIUMINT	= "mediumint"
	TYPE_INT		= "int"
	TYPE_BIGINT		= "bigint"
)

var (
	db_tables schema_tables
	
	integers = map[string]int{
		TYPE_TINYINT:		int_pow(2, 8),
		TYPE_SMALLINT:		int_pow(2, 16),
		TYPE_MEDIUMINT:		int_pow(2, 24),
		TYPE_INT:			int_pow(2, 32),
		TYPE_BIGINT:		int_pow(2, 64),
	}
	
	schema_int 		= regexp.MustCompile(`^(`+TYPE_TINYINT+`|`+TYPE_SMALLINT+`|`+TYPE_MEDIUMINT+`|`+TYPE_INT+`|`+TYPE_BIGINT+`)\((\d+)\)(?: (.*))?`)
	schema_char 	= regexp.MustCompile(`^(varchar|char)\((\d+)\)`)
	schema_decimal 	= regexp.MustCompile(`^(decimal)\((\d+),(\d+)\)(?: (.*))?`)
	schema_enum 	= regexp.MustCompile(`^(enum)\((.*)\)`)
)

type (
	schema_tables	map[string]schema_table
	schema_table 	map[string]schema_column
	
	schema_column struct {
		Type		string
		Subtype		string
		Length		int
		Length_dec 	int
		Unsigned 	bool
		Null		bool
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
)

func Fetch_schema(){
	db_tables = schema_tables{}
	
	rows, err := db.QueryContext(context.Background(), "SHOW TABLES")
	if err != nil {
		log.Fatalf("Unable to fetch DB schema: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Fatalf("Unable to fetch DB schema tables: %v", err)
		}
		fetch_schema_table(table)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Unable to fetch DB schema tables: %v", err)
	}
}

func Schema(table, column string) schema_column {
	col_schema, found := db_tables[table][column]
	if !found {
		panic("Unable to lookup schema column: "+table+"."+column)
	}
	return col_schema
}

func fetch_schema_table(table string){
	table_cols := schema_table{}
	
	rows, err := db.QueryContext(context.Background(), "SHOW COLUMNS FROM ."+table)
	if err != nil {
		log.Fatalf("Unable to fetch DB schema table: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			column 	string
			format 	string
			null 	string
			key 	string
			def 	*string
			extra 	string
		)
		if err := rows.Scan(&column, &format, &null, &key, &def, &extra); err != nil {
			log.Fatalf("Unable to fetch DB schema table column: %v", err)
		}
		
		var (
			is_null			= null == "YES"
			is_unsigned 	bool
		)
		
		if matches := schema_int.FindStringSubmatch(format); len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			is_unsigned = check_unsigned(matches[3])
			
			var (
				min int
				int_range = integers[matches[1]]
			)
			if !is_unsigned {
				min = int_range / -2
			}
			
			table_cols[column] = schema_column{
				Type:		SCHEMA_INT,
				Subtype:	matches[1],
				Length:		length,
				Unsigned:	is_unsigned,
				Null:		is_null,
				Range:		length_range{min, min + int_range - 1},
			}
			continue
		}
		
		if matches := schema_char.FindStringSubmatch(format); len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			
			table_cols[column] = schema_column{
				Type:		SCHEMA_CHAR,
				Subtype:	matches[1],
				Length:		length,
				Null:		is_null,
			}
			continue
		}
		
		if matches := schema_decimal.FindStringSubmatch(format); len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			dec, _ := strconv.Atoi(matches[3])
			is_unsigned = check_unsigned(matches[4])
			min, max := decimal_range(length, dec, is_unsigned)
			
			table_cols[column] = schema_column{
				Type:		SCHEMA_DEC,
				Subtype:	matches[1],
				Length:		length,
				Length_dec:	dec,
				Unsigned:	is_unsigned,
				Null:		is_null,
				Range_dec:	length_range_dec{min, max},
			}
			continue
		}
		
		if matches := schema_enum.FindStringSubmatch(format); len(matches) != 0 {
			table_cols[column] = schema_column{
				Type:		SCHEMA_CHAR,
				Subtype:	matches[1],
				Null:		is_null,
			}
			continue
		}
		
		log.Fatal("Unknown column: "+column+" "+format)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Unable to fetch DB schema table column: %v", err)
	}
	
	db_tables[table] = table_cols
}

func decimal_range(length int, dec int, unsigned bool) (float64, float64){
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

func check_unsigned(s string) bool {
	return s == "unsigned"
}