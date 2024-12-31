# Install
`go get -u github.com/clarkk/go-dbd`

All packages are extremely simple and lightweight by design

- [go-dbd/sqlc](#go-dbdsqlc) SQL compiler

# go-dbd/sqlc
Compile complex MySQL queries as prepared statements.

### Example
```
package main

import (
  "fmt"
  "github.com/clarkk/go-dbd/sqlc"
)

func main(){
  select_query()
  select_join_query()
  insert_query()
  update_query()
  delete_query()
}

func select_query(){
  query := sqlc.NewSelect("user").
    Select([]string{
      "id",
      "name",
      "email",
    }).
    Where(sqlc.Where().
      Eq("email", "test@domain.com"))
  
  fmt.Println(query.Compile(), query.Data())
  /*
    SELECT id, name, email
    FROM .user
    WHERE email=?
  */
}

func select_join_query(){
  query := sqlc.NewSelect("user").
    Select([]string{
      "id",
      "c.timeout",
      "name",
      "email",
    }).
    Left_join("client", "c", "id", "client_id").
    Where(sqlc.Where().
      Eq("email", "test@domain.com"))
  
  fmt.Println(query.Compile(), query.Data())
  /*
    SELECT u.id, c.timeout, u.name, u.email
    FROM .user u
    LEFT JOIN .client c ON c.id=u.client_id
    WHERE u.email=?
  */
}

func insert_query(){
  query := sqlc.NewInsert("user").
    Fields(map[string]any{
      "name":   "john",
      "email":  "alias@test.com",
    })
  
  fmt.Println(query.Compile(), query.Data())
  /*
    INSERT .user
    SET name=?, email=?
  */
}

func update_query(){
  query := NewUpdate("user").
    Fields(map[string]any{
      "name": "michael",
    }).
    Where(Where().
      Eq("id", 100))
  
  fmt.Println(query.Compile(), query.Data())
  /*
    UPDATE .user
    SET name=?
    WHERE id=?
  */
}

func delete_query(){
  query := NewDelete("user").
    Where(Where().
      Eq("id", 100))
  
  fmt.Println(query.Compile(), query.Data())
  /*
    DELETE FROM .user
    WHERE id=?
  */
}
```
