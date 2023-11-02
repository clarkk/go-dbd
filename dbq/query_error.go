package dbq

import(
	"fmt"
	//"strings"
	"github.com/go-errors/errors"
)

const (
	ERR_CODE_PRIVATE Error_code 		= 1
	ERR_CODE_INVALID_FIELDS Error_code 	= 2
)

type (
	Error_code 		uint8
)

func (q *Query) error_table_private() (Error_code, error) {
	return ERR_CODE_PRIVATE, errors.New("Table private")
}

func (q *Query) error_invalid_field(name string){
	q.error_code 			= ERR_CODE_INVALID_FIELDS
	q.invalid_fields[name]	= fmt.Sprintf(`Field translation missing in '%s' for field: %s`, q.table_name, name)
}

/*func (q *query) error() (Error_code, error) {
	if q.error_code != 0 {
		switch q.error_code {
		
		case ERR_CODE_INVALID_FIELDS:
			var msg string
			values := make([]string, len(q.invalid_fields))
			i := 0
			if q.public {
				for k := range q.invalid_fields {
					values[i] = k
					i++
				}
				msg = fmt.Sprintf("Invalid fields: %s", strings.Join(values, ", "))
			}else{
				for _, v := range q.invalid_fields {
					values[i] = v
					i++
				}
				msg = strings.Join(values, ", ")
			}
			return q.error_code, errors.New(msg)
			
		default:
			return q.error_code, errors.New("Unspecified error")
		}
	}
	
	return 0, nil
}*/