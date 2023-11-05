# Install
`go get -u github.com/clarkk/go-dbd`

# go-dbd
Lightweight and flexible SQL builder with models

### Example
```
package main

import (
  "context"
  t "github.com/clarkk/go-dbd/dbt"
  "github.com/clarkk/go-dbd/dbc"
  "github.com/clarkk/go-dbd/dbv"
)

//  Model for "client"
var Client = t.NewTable(
  "client",
  t.Fields{
    "id":            t.Field{"client", "id"},
    "is_suspended":  t.Field{"client", "is_suspended"},
    "time_created":  t.Field{"client", "time_created"},
    "timeout":       t.Field{"client", "timeout"},
    "lang":          t.Field{"client", "lang"},
  },
  t.Joins{},
  t.Get{},
  t.Put{},
)

//  Model for "user"
var User = t.NewTable(
  "user",
  t.Fields{
    "id":            t.Field{"user", "id"},
    "client_id":     t.Field{"user", "client_id"},
    "is_suspended":  t.Field{"client", "is_suspended"},
    "name":          t.Field{"user", "name"},
    "email":         t.Field{"user", "email"},
  },
  t.Joins{
    "client":        t.Join{t.LEFT_JOIN, "client_id", "id"},
  },
  t.Get{
    "id",
    "is_suspended",
    "name",
  },
  t.Put{
    
  },
)

//  Collection of models
var App = dbc.NewCollection().Apply(dbv.NewView(
  Client,
  false,  // not public
)).Apply(dbv.NewView(
  User,
  true,   // public
))

func main(){
  query, err := App.Get(context.Context(), "user")
}
```

## Select query
```

```

## Prepared statement
```

```