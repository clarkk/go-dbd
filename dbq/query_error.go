package dbq

import(
	"fmt"
	"strings"
	//"github.com/go-errors/errors"
)

type (
	Error struct{
		code 	Error_code
		msg 	string
	}
	
	Error_code 	uint8
)

const (
	ERR_CODE_SELECT_EMPTY Error_code 	= 1
	ERR_CODE_FIELDS_INVALID Error_code 	= 2
	ERR_CODE_WHERE_OPERATOR Error_code 	= 3
	ERR_CODE_WHERE_VALUE Error_code 	= 4
	ERR_CODE_ORDER_MODE Error_code 		= 5
	ERR_CODE_LIMIT_VALUE Error_code 	= 6
	ERR_CODE_SELECT_LOCK_ID Error_code 	= 7
)

func (e *Error) Code() Error_code {
	return e.code
}

func (e *Error) Error() string {
	return e.msg
}

func (q *query) error() error {
	if q.error_code == 0 {
		return nil
	}
	
	switch q.error_code {
	case ERR_CODE_FIELDS_INVALID:
		var msg string
		values := make([]string, len(q.invalid_fields))
		i := 0
		if q.public {
			for k := range q.invalid_fields {
				values[i] = k
				i++
			}
			msg = fmt.Sprintf("Fields invalid: %s", strings.Join(values, ", "))
		}else{
			for _, v := range q.invalid_fields {
				values[i] = v
				i++
			}
			msg = strings.Join(values, ", ")
		}
		return &Error{q.error_code, msg}
		
	case ERR_CODE_WHERE_OPERATOR:
		return &Error{q.error_code, fmt.Sprintf("Where operators invalid: %s", strings.Join(q.invalid_where_operator, ", "))}
		
	case ERR_CODE_WHERE_VALUE:
		return &Error{q.error_code, fmt.Sprintf("Where values invalid: %s", strings.Join(q.invalid_where, ", "))}
		
	case ERR_CODE_ORDER_MODE:
		return &Error{q.error_code, fmt.Sprintf("Order modes invalid: %s", strings.Join(q.invalid_order_mode, ", "))}
		
	default:
		return &Error{q.error_code, "Unspecified error"}
	}
}

func (q *query) error_table_private() error {
	return fmt.Errorf("Table private")
}

func (q *query) error_select_empty() error {
	return &Error{ERR_CODE_SELECT_EMPTY, "Select empty"}
}

func (q *query) error_invalid_field(name string){
	q.error_code 			= ERR_CODE_FIELDS_INVALID
	q.invalid_fields[name]	= fmt.Sprintf(`Field translation missing in '%s' for field: %s`, q.table_name, name)
}

func (q *query) error_where_operator(name string, operator string){
	q.error_code 				= ERR_CODE_WHERE_OPERATOR
	q.invalid_where_operator	= append(q.invalid_where_operator, fmt.Sprintf("%s %s", name, operator))
}

func (q *query) error_where_value(name string){
	q.error_code 			= ERR_CODE_WHERE_VALUE
	q.invalid_where			= append(q.invalid_where, name)
}

func (q *query) error_order_mode(mode string){
	q.error_code 			= ERR_CODE_ORDER_MODE
	q.invalid_order_mode 	= append(q.invalid_order_mode, mode)
}

func (q *query) error_limit_value(){
	q.error_code 			= ERR_CODE_LIMIT_VALUE
}

func (q *query) error_select_lock_id() error {
	return &Error{ERR_CODE_SELECT_LOCK_ID, "Read lock is only supported with where by id"}
}