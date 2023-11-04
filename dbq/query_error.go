package dbq

import(
	"fmt"
	"strings"
	"github.com/go-errors/errors"
)

const (
	ERR_CODE_SUCCESS Error_code 		= 0
	ERR_CODE_PRIVATE Error_code 		= 1
	ERR_CODE_SELECT_EMPTY Error_code 	= 2
	ERR_CODE_FIELDS_INVALID Error_code 	= 3
	ERR_CODE_WHERE_OPERATOR Error_code 	= 4
	ERR_CODE_WHERE_VALUE Error_code 	= 5
	ERR_CODE_LIMIT_VALUE Error_code 	= 6
	ERR_CODE_SELECT_LOCK_ID Error_code 	= 7
)

type (
	Error_code 		uint8
)

func (q *Query) error() (Error_code, error) {
	if q.error_code == 0 {
		return 0, nil
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
		return q.error_code, errors.New(msg)
		
	case ERR_CODE_WHERE_VALUE:
		return q.error_code, errors.New(
			fmt.Sprintf("Where values invalid: %s", strings.Join(q.invalid_where, ", ")),
		)
		
	default:
		return q.error_code, errors.New("Unspecified error")
	}
}

func (q *Query) error_table_private() (Error_code, error) {
	return ERR_CODE_PRIVATE, errors.New("Table private")
}

func (q *Query) error_select_empty() (Error_code, error) {
	return ERR_CODE_SELECT_EMPTY, errors.New("Select empty")
}

func (q *Query) error_invalid_field(name string){
	q.error_code 			= ERR_CODE_FIELDS_INVALID
	q.invalid_fields[name]	= fmt.Sprintf(`Field translation missing in '%s' for field: %s`, q.table_name, name)
}

func (q *Query) error_where_operator(name string, operator string){
	q.error_code 				= ERR_CODE_WHERE_OPERATOR
	q.invalid_where_operator	= append(q.invalid_where, fmt.Sprintf(`Where operators invalid: %s %s`, name, operator))
}

func (q *Query) error_where_value(name string){
	q.error_code 			= ERR_CODE_WHERE_VALUE
	q.invalid_where			= append(q.invalid_where, fmt.Sprintf(`Where values invalid: %s`, name))
}

func (q *Query) error_limit_value(){
	q.error_code 			= ERR_CODE_LIMIT_VALUE
}

func (q *Query) error_select_lock_id() (Error_code, error) {
	return ERR_CODE_SELECT_LOCK_ID, errors.New("Read lock is only supported with where by id")
}