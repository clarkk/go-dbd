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

query := sqlc.Select("user").
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(sqlc.Where().
    Eq("name", "test").
    Eq("email", "test@domain.com"))

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
```
SELECT id, name, email
FROM .user
WHERE name='test' && email='test@domain.com'
``` 

## Select query with Eqs()
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := sqlc.Select("user").
  Select([]string{
    "id",
    "name",
    "email",
  }).
  Where(sqlc.Where().Eqs(sqlc.Map{
    "name":   "test",
    "email":  "test@domain.com",
  }))

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
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

query := sqlc.Select("user").
  Select([]string{
    "id",
    "c.timeout",
    "name",
    "email",
  }).
  Left_join("client", "c", "id", "client_id").
  Where(sqlc.Where().
    Eq("email", "test@domain.com"))

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
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

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
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

query := Update("user").
  Fields(sqlc.Map{
    "name": "michael",
  }).
  Where(Where().
    Eq("id", 100))

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
```
UPDATE .user
SET name='michael'
WHERE id=100
```

## Delete query
```
import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

query := Delete("user").
  Where(Where().
    Eq("id", 100))

fmt.Println(query.Compile(), query.Data(), sqlc.SQL_debug(query))
```

### SQL query
```
DELETE FROM .user
WHERE id=100
```

## Where
- Equal (`x=?`) `Eq(field string, value any)`
- Equal (`x=?`) `Eqs(fields map[string]any)`
- Greater than (`x>?`) `Gt(field string, value any)`
- Greater than or equal to (`x>=?`) `Gt_eq(field string, value any)`
- Less than (`x<?`) `Lt(field string, value any)`
- Less than or equal to (`x<=?`) `Lt_eq(field string, value any)`
- Between (`x BETWEEN ? AND ?`) `Bt(field string, value1, value2 any)`
- Not between (`x NOT BETWEEN ? AND ?`) `Not_bt(field string, value1, value2 any)`
- In (`x IN (?,?,?)`) `In(field string, values []any)`
- Not in (`x NOT IN (?,?,?)`) `Not_in(field string, values []any)`

### Example
```
sqlc.Where().
  Eq("id", 100).
  Gt("id", 200).
  Gt_eq("id", 300).
```

### SQL where clause
```
WHERE id=100 && id>200 && id>=300
```