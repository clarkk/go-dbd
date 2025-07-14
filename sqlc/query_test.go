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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
	
	t.Run("select function", func(t *testing.T){
		query := Select("user").
			Select([]string{
				"count|id",
			}).
			Group([]string{
				"id",
			})
		
		sql, _ := query.Compile()
		
		want :=
`SELECT count(id)
FROM .user
GROUP BY id`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT count(id)
FROM .user
GROUP BY id`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select id empty", func(t *testing.T){
		query := Select_id("user", 0).
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
WHERE id=0 && email>? && email<?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=0 && email>test1 && email<test2
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select id set", func(t *testing.T){
		query := Select_id("user", 123).
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
WHERE id=123 && email>? && email<?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=123 && email>test1 && email<test2
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("where operator compatability opposite", func(t *testing.T){
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
		query := Select("user").
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
	
	t.Run("select in subquery", func(t *testing.T){
		subquery := Select("user").
			Select([]string{
				"id",
			}).
			Where(Where().
				Eq("name", "subquery_value"),
			)
		
		query := Select("user").
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				In_subquery("id", subquery).
				Eq("name", "9"),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE id IN (SELECT id
FROM .user
WHERE name=?
) && name=?
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id IN (SELECT id
FROM .user
WHERE name=subquery_value
) && name=9
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select not in", func(t *testing.T){
		query := Select("user").
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
		query := Select_id("user", 123).
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
		query := Select("user").
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
	
	t.Run("select for update", func(t *testing.T){
		query := Select_id("user", 123).
			Select([]string{
				"id",
				"email",
			}).
			Read_lock()
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE id=123
FOR UPDATE`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE id=123
FOR UPDATE`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_insert(t *testing.T){
	t.Run("insert", func(t *testing.T){
		query := Insert("user").
			Fields(Map{
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
	
	t.Run("insert update duplicate", func(t *testing.T){
		query := Insert("user").
			Update_duplicate(nil).
			Fields(Map{
				"time_login":	123,
				"name":			"test",
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .user
SET time_login=?, name=?
ON DUPLICATE KEY UPDATE time_login=?, name=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .user
SET time_login=123, name=test
ON DUPLICATE KEY UPDATE time_login=123, name=test`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("insert update duplicate fields", func(t *testing.T){
		query := Insert("user").
			Update_duplicate([]string{
				"time_login",
			}).
			Fields(Map{
				"time_login":	123,
				"name":			"test",
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .user
SET time_login=?, name=?
ON DUPLICATE KEY UPDATE time_login=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .user
SET time_login=123, name=test
ON DUPLICATE KEY UPDATE time_login=123`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_inserts(t *testing.T){
	t.Run("inserts", func(t *testing.T){
		query := Inserts("account").
			Fields(Map{
				"account_number":	123,
				"name":				"test1",
			}).
			Fields(Map{
				"account_number":	456,
				"name":				"test2",
			}).
			Fields(Map{
				"account_number":	789,
				"name":				"test3",
			}).
			Fields(Map{
				"account_number":	101112,
				"name":				"test4",
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .account (account_number, name)
VALUES (?, ?),(?, ?),(?, ?),(?, ?)`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .account (account_number, name)
VALUES (123, test1),(456, test2),(789, test3),(101112, test4)`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("inserts update duplicate", func(t *testing.T){
		query := Inserts("account").
			Update_duplicate(nil).
			Fields(Map{
				"account_number":	123,
				"name":				"test1",
			}).
			Fields(Map{
				"account_number":	456,
				"name":				"test2",
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .account (account_number, name)
VALUES (?, ?),(?, ?)
ON DUPLICATE KEY UPDATE account_number=VALUES(account_number), name=VALUES(name)`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .account (account_number, name)
VALUES (123, test1),(456, test2)
ON DUPLICATE KEY UPDATE account_number=VALUES(account_number), name=VALUES(name)`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("inserts update duplicate fields", func(t *testing.T){
		query := Inserts("account").
			Update_duplicate([]string{
				"name",
			}).
			Fields(Map{
				"account_number":	123,
				"name":				"test1",
			}).
			Fields(Map{
				"account_number":	456,
				"name":				"test2",
			})
		
		sql, _ := query.Compile()
		
		want :=
`INSERT .account (account_number, name)
VALUES (?, ?),(?, ?)
ON DUPLICATE KEY UPDATE name=VALUES(name)`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`INSERT .account (account_number, name)
VALUES (123, test1),(456, test2)
ON DUPLICATE KEY UPDATE name=VALUES(name)`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_update(t *testing.T){
	t.Run("update id empty", func(t *testing.T){
		query := Update_id("user", 0).
			Fields(Map{
				"time_login": 123,
			})
		
		sql, _ := query.Compile()
		
		want :=
`UPDATE .user
SET time_login=?
WHERE id=0`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`UPDATE .user
SET time_login=123
WHERE id=0`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("update id set", func(t *testing.T){
		query := Update_id("user", 100).
			Fields(Map{
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
	
	t.Run("update", func(t *testing.T){
		query := Update("user").
			Fields(Map{
				"time_login": 123,
			})
		
		sql, _ := query.Compile()
		
		want :=
`UPDATE .user
SET time_login=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`UPDATE .user
SET time_login=123`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_delete(t *testing.T){
	t.Run("delete id empty", func(t *testing.T){
		query := Delete_id("user", 0)
		
		sql, _ := query.Compile()
		
		want :=
`DELETE FROM .user
WHERE id=0`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`DELETE FROM .user
WHERE id=0`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("delete id set", func(t *testing.T){
		query := Delete_id("user", 100)
		
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
	
	t.Run("delete", func(t *testing.T){
		query := Delete("user")
		
		sql, _ := query.Compile()
		
		want :=
`DELETE FROM .user`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`DELETE FROM .user`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_select_json(t *testing.T){
	t.Run("json table", func(t *testing.T){
		j := JSON_table("j").
			Source_key_value("p.document_languages", "$[*]").
			Column_path("lang_id", "int unsigned", "$.key").
			Column_path("description", "text", "$.value.description")
		
		query := Select_id("payment_term", 34).
			Select([]string{
				"d.language",
				"j.description",
			}).
			JSON_table(j).
			Left_join("document_language", "d", "id", "j.lang_id")
		
		sql, _ := query.Compile()
		
		want :=
`SELECT d.language, j.description
FROM .payment_term p, JSON_TABLE(
	JSON_KEY_VALUE(p.document_languages, '$'), '$[*]'
	COLUMNS (lang_id int unsigned PATH '$.key', description text PATH '$.value.description')
) j
LEFT JOIN .document_language d ON d.id=j.lang_id
WHERE p.id=34`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT d.language, j.description
FROM .payment_term p, JSON_TABLE(
	JSON_KEY_VALUE(p.document_languages, '$'), '$[*]'
	COLUMNS (lang_id int unsigned PATH '$.key', description text PATH '$.value.description')
) j
LEFT JOIN .document_language d ON d.id=j.lang_id
WHERE p.id=34`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}

func Test_update_json(t *testing.T){
	t.Run("json remove", func(t *testing.T){
		query := Update_id("payment_term", 34).
			JSON_remove("document_languages", "$.784")
		
		sql, _ := query.Compile()
		
		want :=
`UPDATE .payment_term
SET document_languages=JSON_REMOVE(document_languages, '$.784')
WHERE id=34`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`UPDATE .payment_term
SET document_languages=JSON_REMOVE(document_languages, '$.784')
WHERE id=34`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
}