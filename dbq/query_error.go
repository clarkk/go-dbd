package dbq

import(
	"fmt"
	"strings"
	"github.com/go-errors/errors"
)

const (
	ERR_CODE_PRIVATE error_code 		= 1
	ERR_CODE_INVALID_FIELDS error_code 	= 2
)

func (q *query) error() (error_code, error) {
	if q.error_code != 0 {
		switch q.error_code {
		case ERR_CODE_PRIVATE:
			return q.error_code, errors.New("Table private")
			
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
}

func (q *query) error_private(){
	q.error_code = ERR_CODE_PRIVATE
}

func (q *query) error_invalid_field(name string){
	q.error_code 			= ERR_CODE_INVALID_FIELDS
	q.invalid_fields[name]	= fmt.Sprintf(`Field translation missing in '%s' for field: %s`, q.view.Table().Name(), name)
}