package sqlc

/*
	Test
	# go test . -v
*/

import (
	"strings"
	"testing"
)

func Test_select(t *testing.T){
	t.Run("table collisions", func(t *testing.T){
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
			)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE a.email=? && u.time IN (?,?,?)`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE a.email=test1 && u.time IN (1,2,3)`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("select", func(t *testing.T){
		query := Select("user").
			Select([]string{
				"id",
				"email",
			}).
			Where(Where().
				Eq("email", "test1").
				Gt("email", "test2").
				Gt_eq("email", "test3").
				Lt("email", "test4").
				Lt_eq("email", "test5").
				Bt("email", "1", "2").
				Not_bt("email", "3", "4").
				In("email", []any{"5","6","7"}).
				Not_in("email", []any{"8","9","10"}),
			).
			Limit(0, 10)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT id, email
FROM .user
WHERE email=? && email>? && email>=? && email<? && email<=? && email BETWEEN ? AND ? && email NOT BETWEEN ? AND ? && email IN (?,?,?) && email NOT IN (?,?,?)
LIMIT 0,10`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email=test1 && email>test2 && email>=test3 && email<test4 && email<=test5 && email BETWEEN 1 AND 2 && email NOT BETWEEN 3 AND 4 && email IN (5,6,7) && email NOT IN (8,9,10)
LIMIT 0,10`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
	t.Run("where eqs", func(t *testing.T){
		query := Select("user").
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
WHERE email=? && name=?`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT id, email
FROM .user
WHERE email=test1 && name=test2`
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
				Gt("c.timeout", "test2").
				Gt_eq("c.timeout", "test3").
				Lt("c.timeout", "test4").
				Lt_eq("c.timeout", "test5").
				Bt("c.timeout", "1", "2").
				Not_bt("c.timeout", "3", "4").
				In("c.timeout", []any{"5","6","7"}).
				Not_in("c.timeout", []any{"8","9","10"}),
			)
		
		sql, _ := query.Compile()
		
		want :=
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=? && c.timeout>? && c.timeout>=? && c.timeout<? && c.timeout<=? && c.timeout BETWEEN ? AND ? && c.timeout NOT BETWEEN ? AND ? && c.timeout IN (?,?,?) && c.timeout NOT IN (?,?,?)`
		got := strings.TrimSpace(sql)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
		
		want =
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=test1 && c.timeout>test2 && c.timeout>=test3 && c.timeout<test4 && c.timeout<=test5 && c.timeout BETWEEN 1 AND 2 && c.timeout NOT BETWEEN 3 AND 4 && c.timeout IN (5,6,7) && c.timeout NOT IN (8,9,10)`
		got = SQL_debug(query)
		if got != want {
			t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
		}
	})
	
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
	
	t.Run("update", func(t *testing.T){
		query := Update("user").
			Fields(map[string]any{
				"time_login": 123,
			}).
			Where(Where().
				Eq("id", "100"),
			)
		
		sql, _ := query.Compile()
		
		want :=
`UPDATE .user
SET time_login=?
WHERE id=?`
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
	
	t.Run("delete", func(t *testing.T){
		query := Delete("user").
			Where(Where().
				Eq("id", "100"),
			)
		
		sql, _ := query.Compile()
		
		want :=
`DELETE FROM .user
WHERE id=?`
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