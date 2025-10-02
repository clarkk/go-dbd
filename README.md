# Install
`go get -u github.com/clarkk/go-dbd`

Database handling for MySQL/MariaDB

All packages are extremely simple and lightweight by design

- [go-dbd/sqlc](#go-dbdsqlc) SQL compiler

# go-dbd/sqlc
Compile complex MySQL queries as prepared statements.

## Select query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

where := sqlc.Where().
  Eq("name", "test").
  Eq("email", "test@domain.com")

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(where).
  Order([]string{
    "name",
    "id DESC",
  }).
  Limit(0, 10)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT id, name, email
FROM .user
WHERE name='test' && email='test@domain.com'
ORDER BY name, id DESC
LIMIT 0,10
``` 

## Select query by id
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Select("user", 123).
  Select([]string{
    "id",
    "name",
    "email",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT id, name, email
FROM .user
WHERE id=123
``` 

## Select for update (read lock)
Select rows with read lock until the transaction has finished (commit/rollback) to avoid race conditions.
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Select("user", 123).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Read_lock()

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT id, name, email
FROM .user
WHERE id=123
FOR UPDATE
``` 

## Select query with Eqs()
Instead of using multiple `Eq()` they can all be added at once in a map via `Eqs()`
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

where := sqlc.Where().Eqs(sqlc.Map{
  "name":   "test",
  "email":  "test@domain.com",
})

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(where)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT id, name, email
FROM .user
WHERE name='test' && email='test@domain.com'
``` 

## Select join query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

where := sqlc.Where().Eqs(sqlc.Map{
  "name":      "test",
  "email":     "test@domain.com",
  "c.active":  1,
})

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "c.timeout",
    "name",
    "email",
  }).
  Left_join("client", "c", "id", "client_id").
  Where(where)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT u.id, c.timeout, u.name, u.email
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.name='test' && u.email='test@domain.com' && c.active=1
```

## Select where with sub-query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

subquery := sqlc.Select("user").
  Select([]string{
    "id",
  }).
  Where(sqlc.Where().
    Eq("name", "subquery_value"),
  )

where := sqlc.Where().
  In_subquery("id", subquery).

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(where)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT id, name, email
FROM .user
WHERE id IN (SELECT id
FROM .user
WHERE name='subquery_value'
)
```

## Select wrap where
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

where_inner := sqlc.Where().
  Eq("u.inner", "test1")

where_middle := sqlc.Where().
  Eq("middle", "test2")

where_middle.Wrap(where_inner)

where_outer := sqlc.Where().
  Eq("outer", "test3")

where_outer.Wrap(where_middle)

query := sqlc.Select("user").
  Select([]string{
    "id",
    "email",
    "u.time",
  }).
  Left_join("user_block", "u", "id", "user_id").
  Where(where_outer)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE u.inner='test1' && a.middle='test2' && a.outer='test3'
```

## Insert query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Insert("user").
  Fields(sqlc.Map{
    "name":   "john",
    "email":  "alias@test.com",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
INSERT .user
SET name='john', email='alias@test.com'
```

## Insert on duplicate update query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Insert("user").
  Update_duplicate(nil).
  Fields(sqlc.Map{
    "name":   "john",
    "email":  "alias@test.com",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
INSERT .user
SET name='john', email='alias@test.com'
ON DUPLICATE KEY UPDATE name='john', email='alias@test.com'
```

## Insert on duplicate update query (update specific fields)
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Insert("user").
  Update_duplicate([]string{
    "name",
  }).
  Fields(sqlc.Map{
    "name":   "john",
    "email":  "alias@test.com",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
INSERT .user
SET name='john', email='alias@test.com'
ON DUPLICATE KEY UPDATE name='john'
```

## Insert on duplicate update query (with operators)
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

fields := sqlc.Fields().
  Add("balance", 12).
  Sub("deleted", 10)

query := sqlc.Insert("sum").
  Update_duplicate_operator(fields, nil)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
INSERT .sum
SET balance=12, deleted=-10
ON DUPLICATE KEY UPDATE balance=balance+12, deleted=deleted-10
```

## Insert multiple
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Inserts("account").
  Fields(sqlc.Map{
    "account":  123,
    "name":     "test1",
  }).
  Fields(sqlc.Map{
    "account":  456,
    "name":     "test2",
  }).
  Fields(sqlc.Map{
    "account":  789,
    "name":     "test3",
  }).
  Fields(sqlc.Map{
    "account":  101112,
    "name":     "test4",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
INSERT .account (account, name)
VALUES (123, test1),(456, test2),(789, test3),(101112, test4)
```

## Update query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Update("user", 123).
  Fields(sqlc.Map{
    "name": "michael",
  })

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
UPDATE .user
SET name='michael'
WHERE id=123
```

## Delete query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Delete("user", 123)

sql, err := query.Compile()
if err != nil {
  panic(err)
}

fmt.Println(sql, query.Data(), sqlc.SQL_debug(query))
```

### SQL
```
DELETE FROM .user
WHERE id=123
```

## Where clause
- **Equal** (`x=?`) `Eq(field string, value any)`
- **Not equal** (`x!=?`) `Not_eq(field string, value any)`
- **Multiple equals** (`x=? && y=? && ...`) `Eqs(fields map[string]any)`
- **Greater than** (`x>?`) `Gt(field string, value any)`
- **Greater than or equal to** (`x>=?`) `Gt_eq(field string, value any)`
- **Less than** (`x<?`) `Lt(field string, value any)`
- **Less than or equal to** (`x<=?`) `Lt_eq(field string, value any)`
- **Is null** (`x IS NULL`) `Null(field string)`
- **Is not null** (`x IS NOT NULL`) `Not_null(field string)`
- **Between** (`x BETWEEN ? AND ?`) `Bt(field string, value1, value2 any)`
- **Not between** (`x NOT BETWEEN ? AND ?`) `Not_bt(field string, value1, value2 any)`
- **In** (`x IN (?,?,?)`) `In(field string, values []any)`
- **Not in** (`x NOT IN (?,?,?)`) `Not_in(field string, values []any)`

### Example
```
sqlc.Where().
  Eq("id", 100).
  Gt("id", 200).
  Gt_eq("id", 300)

sqlc.Where().
  Eqs(sqlc.Map{
    "id": 100,
    "id", 200,
    "id", 300,
  })
```

### SQL where clause
```
WHERE id=100 && id>200 && id>=300

WHERE id=100 && id=200 && id=300
```