package sqlc

import (
	"fmt"
	"reflect"
	"strings"
)

func SQL_error(msg string, q SQL, err error) string {
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func SQL_debug(q SQL) string {
	sql, data, err := q.Compile()
	if err != nil {
		return "Error compiling SQL: "+err.Error()
	}
	
	parts := strings.Split(sql, "?")
	var sb strings.Builder
	
	for i := range len(parts) {
		sb.WriteString(parts[i])
		
		if i < len(data) {
			value	:= data[i]
			s		:= "<nil>"
			
			if value != nil {
				val := reflect.ValueOf(value)
				if val.Kind() == reflect.Ptr {
					if !val.IsNil() {
						s = fmt.Sprintf("%v", val.Elem().Interface())
					}
				} else {
					s = fmt.Sprintf("%v", value)
				}
			}
			sb.WriteString(s)
		}
	}
	
	return strings.TrimSpace(sb.String())
}