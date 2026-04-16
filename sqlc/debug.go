package sqlc

import (
	"fmt"
	"strings"
)

func SQL_debug(q SQL) string {
	sql, data, err := q.Compile()
	if err != nil {
		return "Error compiling SQL: "+err.Error()
	}
	for _, value := range data {
		var actual any = value
		switch t := value.(type) {
		case *int:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *uint:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *int8:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *uint8:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *int32:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *uint32:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *int64:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *uint64:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *float32:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *float64:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *string:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case *bool:
			if t != nil {
				actual = *t
			} else {
				actual = "<nil>"
			}
		case nil:
			actual = "<nil>"
		}
		
		sql = strings.Replace(sql, "?", fmt.Sprintf("%v", actual), 1)
	}
	return strings.TrimSpace(sql)
}