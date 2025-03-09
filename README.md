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

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(sqlc.Where().
    Eq("name", "test").
    Eq("email", "test@domain.com")).
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

## Select query with Eqs()
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(sqlc.Where().Eqs(sqlc.Map{
    "name":   "test",
    "email":  "test@domain.com",
  }))

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

query := sqlc.Select("user", 0).
  Select([]string{
    "id",
    "c.timeout",
    "name",
    "email",
  }).
  Left_join("client", "c", "id", "client_id").
  Where(sqlc.Where().
    Eq("email", "test@domain.com"))

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
WHERE u.email='test@domain.com'
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