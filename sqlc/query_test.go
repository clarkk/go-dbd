package sqlc

/*
	Test
	# go test . -v
*/

import (
	"strings"
	"testing"
)

func Test_error(t *testing.T){
	t.Run("operator compatability", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Eq("email", "test1").
				Gt("email", "test2"),
			)
		
		_, err := query.Compile()
		
		want := "Where clause operator incompatable on same field (email): = >"
		if err.Error() != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%v", want, err)
		}
	})
	
	t.Run("operator compatability oposite", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Gt("email", "test1").
				Eq("email", "test2"),
			)
		
		_, err := query.Compile()
		
		want := "Where clause operator incompatable on same field (email): > ="
		if err.Error() != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%v", want, err)
		}
	})
}

func Test_select(t *testing.T){
	t.Run("table abbreviation collisions", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
				"u.time",
			}).
			Left_join("user_block", "u", "id", "user_id").
			Where(Where().
				Eq("email", "test1").
				In("u.time", []any{1,2,3}),
			).
			Order([]string{
				"name",
				"u.time DESC",
			})
		
		sql, _ := query.Compile()
		
		want :=
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE a.email=? && u.time IN (?,?,?)
ORDER BY a.name, u.time DESC`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE a.email=test1 && u.time IN (1,2,3)
ORDER BY a.name, u.time DESC`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("where operator compatability oposite", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Gt("email", "test1").
				Lt("email", "test2"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email>? && email<?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email>test1 && email<test2
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select eq", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Eq("email", "test1"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email=test1
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select gt", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Gt("email", "test2"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email>?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email>test2
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select gt eq", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Gt_eq("email", "test3"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email>=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email>=test3
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select lt", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Lt("email", "test4"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email<?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email<test4
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select lt eq", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Lt_eq("email", "test5"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email<=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email<=test5
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select null", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Eq("id", 123).
				Null("email").
				Eq("name", "test"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE id=? && email IS NULL && name=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=123 && email IS NULL && name=test
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select not null", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Eq("id", 123).
				Not_null("email").
				Eq("name", "test"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE id=? && email IS NOT NULL && name=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=123 && email IS NOT NULL && name=test
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select bt", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Bt("email", "1", "2"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email BETWEEN ? AND ?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email BETWEEN 1 AND 2
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select not bt", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Not_bt("email", "3", "4"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email NOT BETWEEN ? AND ?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email NOT BETWEEN 3 AND 4
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select in", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				In("email", []any{"5","6","7"}),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email IN (?,?,?)
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email IN (5,6,7)
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select not in", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Not_in("email", []any{"8","9","10"}),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email NOT IN (?,?,?)
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email NOT IN (8,9,10)
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("where eqs", func(t *testing.T){
		query := Select("user", 123).
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().Eqs(Map{
				"email": "test1",
				"name": "test2",
			}))
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE id=123 && email=? && name=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=123 && email=test1 && name=test2`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select join", func(t *testing.T){
		query := Select("user", 0).
			Select([]string{
				"id",
				"c.timeout",
			}).
			Left_join("client", "c", "id", "client_id").
			Where(Where().
				Eq("email", "test1").
				Gt("c.timeout", "test2"),
			)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=? && c.timeout>?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=test1 && c.timeout>test2`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_insert(t *testing.T){
	t.Run("insert", func(t *testing.T){
		query := Insert("user").
			Fields(map[string]any{
				"time_login": 123,
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .user
SET time_login=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .user
SET time_login=123`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_update(t *testing.T){
	t.Run("update", func(t *testing.T){
		query := Update("user", 100).
			Fields(map[string]any{
				"time_login": 123,
			})
		
		sql, _ := query.Compile()
		
		want :=
`UPDATE .user
SET time_login=?
WHERE id=100`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`UPDATE .user
SET time_login=123
WHERE id=100`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_delete(t *testing.T){
	t.Run("delete", func(t *testing.T){
		query := Delete("user", 100)
		
		sql, _ := query.Compile()
		
		want :=
`DELETE FROM .user
WHERE id=100`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`DELETE FROM .user
WHERE id=100`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}