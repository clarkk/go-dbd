package dbd

import (
	"log"
	"maps"
	"slices"
	"context"
	"regexp"
	"strconv"
	"strings"
)

const (
	SCHEMA_CHAR 	= "char"
	SCHEMA_INT 		= "int"
	SCHEMA_DEC 		= "decimal"
	SCHEMA_TEXT		= "text"
	
	TYPE_TINYINT 	= "tinyint"
	TYPE_SMALLINT	= "smallint"
	TYPE_MEDIUMINT	= "mediumint"
	TYPE_INT		= "int"
	TYPE_BIGINT		= "bigint"
)

var (
	db_tables schema_tables
	
	integers = map[string]integer_range{
		TYPE_TINYINT: {
			min_signed:		-128,
			max_signed:		127,
			max_unsigned:	255,
		},
		TYPE_SMALLINT: {
			min_signed:		-32768,
			max_signed:		32767,
			max_unsigned:	65535,
		},
		TYPE_MEDIUMINT: {
			min_signed:		-8388608,
			max_signed:		8388607,
			max_unsigned:	16777215,
		},
		TYPE_INT: {
			min_signed:		-2147483648,
			max_signed:		2147483647,
			max_unsigned:	4294967295,
		},
		TYPE_BIGINT: {
			min_signed:		-9223372036854775808,
			max_signed:		9223372036854775807,
			max_unsigned:	18446744073709551615,
		},
	}
	
	schema_int 		= regexp.MustCompile(`^(`+TYPE_TINYINT+`|`+TYPE_SMALLINT+`|`+TYPE_MEDIUMINT+`|`+TYPE_INT+`|`+TYPE_BIGINT+`)(?:\((\d+)\))?(?: (.*))?$`)
	schema_char 	= regexp.MustCompile(`^(varchar|char)\((\d+)\)`)
	schema_decimal 	= regexp.MustCompile(`^(decimal)\((\d+),(\d+)\)(?: (.*))?`)
	schema_enum 	= regexp.MustCompile(`^(enum)\((.*)\)`)
	schema_text 	= regexp.MustCompile(`^(tinytext|text|mediumtext|longtext)$`)
)

type (
	schema_tables	map[string]schema_table
	schema_table 	map[string]schema_column
	
	schema_column struct {
		data_type		string
		data_subtype	string
		length			int
		length_dec 		int
		unsigned 		bool
		null			bool
		range_int 		length_range_int
		range_dec 		length_range_dec
	}
	
	integer_range struct {
		min_signed		int64
		max_signed		uint64
		max_unsigned	uint64
	}
	
	length_range_int struct {
		Min 		int64
		Max			uint64
	}
	
	length_range_dec struct {
		Min 		float64
		Max			float64
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

func Exists_schema(table, column string) bool {
	_, found := db_tables[table][column]
	return found
}

func Schema(table, column string) schema_column {
	col_schema, found := db_tables[table][column]
	if !found {
		panic("Unable to lookup table column schema: "+table+"."+column)
	}
	return col_schema
}

func Schema_tables() []string {
	return slices.Sorted(maps.Keys(db_tables))
}

func Schema_table_columns(table string) []string {
	table_schema, found := db_tables[table]
	if !found {
		panic("Unable to lookup table schema: "+table)
	}
	return slices.Collect(maps.Keys(table_schema))
}

func (s schema_column) Length() int {
	return s.length
}

func (s schema_column) Range_int() length_range_int {
	return s.range_int
}

func (s schema_column) Range_dec() length_range_dec {
	return s.range_dec
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
			length := 0
			if matches[2] != "" {
				length, _ = strconv.Atoi(matches[2])
			}
			
			is_unsigned 	= check_unsigned(matches[3])
			
			definition, found := integers[matches[1]]
			if !found {
				log.Fatal("Unknown integer type: " + matches[1])
			}
			
			int_range := length_range_int{
				Min: definition.min_signed,
				Max: definition.max_signed,
			}
			
			if is_unsigned {
				int_range.Min = 0
				int_range.Max = definition.max_unsigned
			}
			
			table_cols[column] = schema_column{
				data_type:		SCHEMA_INT,
				data_subtype:	matches[1],
				length:			length,
				unsigned:		is_unsigned,
				null:			is_null,
				range_int:		int_range,
			}
			continue
		}
		
		if matches := schema_char.FindStringSubmatch(format); len(matches) != 0 {
			length, _ := strconv.Atoi(matches[2])
			
			table_cols[column] = schema_column{
				data_type:		SCHEMA_CHAR,
				data_subtype:	matches[1],
				length:			length,
				null:			is_null,
			}
			continue
		}
		
		if matches := schema_decimal.FindStringSubmatch(format); len(matches) != 0 {
			length, _		:= strconv.Atoi(matches[2])
			dec, _			:= strconv.Atoi(matches[3])
			is_unsigned 	= check_unsigned(matches[4])
			min, max		:= decimal_range(length, dec, is_unsigned)
			
			table_cols[column] = schema_column{
				data_type:		SCHEMA_DEC,
				data_subtype:	matches[1],
				length:			length,
				length_dec:		dec,
				unsigned:		is_unsigned,
				null:			is_null,
				range_dec: length_range_dec{
					Min: min,
					Max: max,
				},
			}
			continue
		}
		
		if matches := schema_enum.FindStringSubmatch(format); len(matches) != 0 {
			table_cols[column] = schema_column{
				data_type:		SCHEMA_CHAR,
				data_subtype:	matches[1],
				null:			is_null,
			}
			continue
		}
		
		if matches := schema_text.FindStringSubmatch(format); len(matches) != 0 {
			table_cols[column] = schema_column{
				data_type:		SCHEMA_TEXT,
				data_subtype:	matches[1],
				null:			is_null,
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

func decimal_range(length, dec int, unsigned bool) (float64, float64){
	l, _ := strconv.ParseFloat(strings.Repeat("9", length), 64)
	d, _ := strconv.ParseFloat("1"+strings.Repeat("0", dec), 64)
	
	var (
		min float64
		max = l / d
	)
	if !unsigned {
		min = -max
	}
	return min, max
}

func check_unsigned(s string) bool {
	return slices.Contains(strings.Fields(s), "unsigned")
}