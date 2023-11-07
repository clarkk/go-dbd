package dbc

import (
	"database/sql"
	"github.com/clarkk/go-dbd/dbq"
)

type get struct {
	query 	*dbq.Query_get
}

//	Read-lock: SELECT ... FOR UPDATE
func (c *get) Read_lock() *get {
	c.query.Read_lock()
	return c
}

//	Count all entries with SELECT COUNT(*) and without LIMIT
func (c *get) Count(tx *sql.Tx) (int, error) {
	return c.query.Count(tx)
}

func (c *get) Public() *get {
	c.query.Public()
	return c
}

func (c *get) Select(fields dbq.Select) *get {
	c.query.Select(fields)
	return c
}

func (c *get) Where(fields dbq.Where) *get {
	c.query.Where(fields)
	return c
}

func (c *get) Limit(fields dbq.Limit) *get {
	c.query.Limit(fields)
	return c
}

func (c *get) Compile() (dbq.Error_code, error) {
	return c.query.Compile()
}

func (c *get) Fetch(tx *sql.Tx) error {
	return c.query.Fetch(tx)
}

func (c *get) Fetch_row(tx *sql.Tx) (dbq.Row_result, error) {
	return c.query.Fetch_row(tx)
}

func (c *get) Next() bool {
	return c.query.Next()
}

func (c *get) Row() dbq.Row_result {
	return c.query.Row()
}

func (c *get) Row_error() error {
	return c.query.Row_error()
}

/*func (c *get) Prepare(tx *sql.Tx) (dbq.Error_code, error) {
	return c.query.Prepare(tx)
}

func (c *get) Result() (sql.Result, error) {
	return c.query.Result()
}*/

func (c *get) Close(){
	c.query.Close()
}

/*rows, err := stmt.QueryContext(ctx, 1)
if err != nil {
	log.Fatal(err)
}
defer rows.Close()

cols, _ := rows.Columns()
cols_len := len(cols)
for rows.Next() {
	columns 	:= make([]any, cols_len)
	columns_ref := make([]any, cols_len)
	for i, _ := range columns {
		columns_ref[i] = &columns[i]
	}
	
	if err := rows.Scan(columns_ref...); err != nil {
		log.Fatal(err)
	}
	
	m := make(map[string]any)
	for i, col_name := range cols {
		val := columns_ref[i].(*any)
		m[col_name] = *val
	}
	
	fmt.Println(m)
}
if err = rows.Err(); err != nil {
	log.Fatal(err)
}*/