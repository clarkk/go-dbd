package sqlc

/*
	Test
	# go test . -v
	
	Benchark
	# go test -benchmem -bench=. -run=^$
*/

import (
	"strings"
	"testing"
)

func Benchmark_error(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_operator_compatibility(b)
		run_operator_compatibility_oposite(b)
	}
}

func Test_error(t *testing.T){
	t.Run("operator compatability", func(t *testing.T){
		run_operator_compatibility(t)
	})
	
	t.Run("operator compatability oposite", func(t *testing.T){
		run_operator_compatibility_oposite(t)
	})
}

func run_operator_compatibility(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%v", want, err)
	}
}

func run_operator_compatibility_oposite(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%v", want, err)
	}
}

func Benchmark_select_where_wrap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_where_wrap(b)
	}
}

func Test_select_where_wrap(t *testing.T){
	t.Run("where wrap", func(t *testing.T){
		run_where_wrap(t)
	})
}

func run_where_wrap(tb testing.TB){
	where_inner := Where().
		Eq("u.inner", "test1")
	
	where_middle := Where().
		Eq("middle", "test2")
	
	where_middle.Wrap(where_inner)
	
	where_outer := Where().
		Eq("outer", "test3")
	
	where_outer.Wrap(where_middle)
	
	query := Select("user").
		Select([]string{
			"id",
			"email",
			"u.time",
		}).
		Left_join("user_block", "u", "id", "user_id", nil).
		Where(where_outer)
	
	sql, _ := query.Compile()
	
	want :=
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE u.inner=? AND a.middle=? AND a.outer=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE u.inner=test1 AND a.middle=test2 AND a.outer=test3`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_select_where_or_group(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_select_where_or_group(b)
	}
}

func Test_select_where_or_group(t *testing.T){
	t.Run("where or group", func(t *testing.T){
		run_select_where_or_group(t)
	})
}

func run_select_where_or_group(tb testing.TB){
	where_or1 := Where().
		Bt("col1", "start1", "end1").
		Bt("col2", "start2", "end2")
	
	where_or2 := Where().
		Bt("col3", "start3", "end3")
	
	where := Where().
		Eq("outer", "test3")
	
	where.Or_group(where_or1)
	where.Or_group(where_or2)
	
	query := Select("user").
		Select([]string{
			"id",
			"email",
			"u.time",
		}).
		Left_join("user_block", "u", "id", "user_id", nil).
		Where(where)
	
	sql, _ := query.Compile()
	
	want :=
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE (a.col1 BETWEEN ? AND ? OR a.col2 BETWEEN ? AND ?) AND (a.col3 BETWEEN ? AND ?) AND a.outer=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE (a.col1 BETWEEN start1 AND end1 OR a.col2 BETWEEN start2 AND end2) AND (a.col3 BETWEEN start3 AND end3) AND a.outer=test3`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_select(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_select_abbreviation_collisions(b)
		run_select_function(b)
		run_select_id_empty(b)
		run_select_id_set(b)
		run_where_operator_compatability_opposite(b)
		run_select_eq(b)
		run_select_gt(b)
		run_select_gt_eq(b)
		run_select_lt(b)
		run_select_lt_eq(b)
		run_select_null(b)
		run_select_not_null(b)
		run_select_bt(b)
		run_select_not_bt(b)
		run_select_in(b)
		run_select_in_subquery(b)
		run_select_not_in(b)
		run_where_eqs(b)
		run_select_join(b)
		run_select_for_update(b)
	}
}

func Test_select(t *testing.T){
	t.Run("table abbreviation collisions", func(t *testing.T){
		run_select_abbreviation_collisions(t)
	})
	
	t.Run("select function", func(t *testing.T){
		run_select_function(t)
	})
	
	t.Run("select id empty", func(t *testing.T){
		run_select_id_empty(t)
	})
	
	t.Run("select id set", func(t *testing.T){
		run_select_id_set(t)
	})
	
	t.Run("where operator compatability opposite", func(t *testing.T){
		run_where_operator_compatability_opposite(t)
	})
	
	t.Run("select eq", func(t *testing.T){
		run_select_eq(t)
	})
	
	t.Run("select gt", func(t *testing.T){
		run_select_gt(t)
	})
	
	t.Run("select gt eq", func(t *testing.T){
		run_select_gt_eq(t)
	})
	
	t.Run("select lt", func(t *testing.T){
		run_select_lt(t)
	})
	
	t.Run("select lt eq", func(t *testing.T){
		run_select_lt_eq(t)
	})
	
	t.Run("select null", func(t *testing.T){
		run_select_null(t)
	})
	
	t.Run("select not null", func(t *testing.T){
		run_select_not_null(t)
	})
	
	t.Run("select bt", func(t *testing.T){
		run_select_bt(t)
	})
	
	t.Run("select not bt", func(t *testing.T){
		run_select_not_bt(t)
	})
	
	t.Run("select in", func(t *testing.T){
		run_select_in(t)
	})
	
	t.Run("select in subquery", func(t *testing.T){
		run_select_in_subquery(t)
	})
	
	t.Run("select not in", func(t *testing.T){
		run_select_not_in(t)
	})
	
	t.Run("where eqs", func(t *testing.T){
		run_where_eqs(t)
	})
	
	t.Run("select join", func(t *testing.T){
		run_select_join(t)
	})
	
	t.Run("select for update", func(t *testing.T){
		run_select_for_update(t)
	})
}

func run_select_abbreviation_collisions(t testing.TB){
	query := Select("user").
		Select([]string{
			"id",
			"email",
			"u.time",
		}).
		Left_join("user_block", "u", "id", "user_id", nil).
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
WHERE a.email=? AND u.time IN (?,?,?)
ORDER BY a.name, u.time DESC`
	got := strings.TrimSpace(sql)
	if got != want {
		t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT a.id, a.email, u.time
FROM .user a
LEFT JOIN .user_block u ON u.id=a.user_id
WHERE a.email=test1 AND u.time IN (1,2,3)
ORDER BY a.name, u.time DESC`
	got = SQL_debug(query)
	if got != want {
		t.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_function(tb testing.TB){
	query := Select("user").
		Select([]string{
			"count|id",
		}).
		Group([]string{
			"id",
		})
	
	sql, _ := query.Compile()
	
	want :=
`SELECT COUNT(id)
FROM .user
GROUP BY id`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT COUNT(id)
FROM .user
GROUP BY id`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_id_empty(tb testing.TB){
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
WHERE id=? AND email>? AND email<?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=0 AND email>test1 AND email<test2
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_id_set(tb testing.TB){
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
WHERE id=? AND email>? AND email<?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=123 AND email>test1 AND email<test2
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_where_operator_compatability_opposite(tb testing.TB){
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
WHERE email>? AND email<?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email>test1 AND email<test2
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_eq(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email=test1
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_gt(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email>test2
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_gt_eq(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email>=test3
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_lt(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email<test4
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_lt_eq(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email<=test5
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_null(tb testing.TB){
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
WHERE id=? AND email IS NULL AND name=?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=123 AND email IS NULL AND name=test
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_not_null(tb	testing.TB){
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
WHERE id=? AND email IS NOT NULL AND name=?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=123 AND email IS NOT NULL AND name=test
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_bt(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email BETWEEN 1 AND 2
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_not_bt(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email NOT BETWEEN 3 AND 4
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_in(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email IN (5,6,7)
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_in_subquery(tb testing.TB){
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
) AND name=?
LIMIT 0,10`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id IN (SELECT id
FROM .user
WHERE name=subquery_value
) AND name=9
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_not_in(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE email NOT IN (8,9,10)
LIMIT 0,10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_where_eqs(tb testing.TB){
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
WHERE id=? AND email=? AND name=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=123 AND email=test1 AND name=test2`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_join(tb testing.TB){
	query := Select("user").
		Select([]string{
			"id",
			"c.timeout",
		}).
		Left_join("client", "c", "id", "client_id", nil).
		Where(Where().
			Eq("email", "test1").
			Gt("c.timeout", "test2"),
		)
	
	sql, _ := query.Compile()
	
	want :=
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=? AND c.timeout>?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT u.id, c.timeout
FROM .user u
LEFT JOIN .client c ON c.id=u.client_id
WHERE u.email=test1 AND c.timeout>test2`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_select_for_update(tb testing.TB){
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
WHERE id=?
FOR UPDATE`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT id, email
FROM .user
WHERE id=123
FOR UPDATE`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_union(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_select_union(b)
	}
}

func Test_union(t *testing.T){
	t.Run("select union", func(t *testing.T){
		run_select_union(t)
	})
}

func run_select_union(tb testing.TB){
	query_union1 := Select("user").
		Select([]string{
			"id col_id",
			"email col_email",
		}).
		Where(Where().
			Eq("col1", 123).
			Gt("email", "test2"),
		)
	
	query_union2 := Select("group").
		Select([]string{
			"id col_id",
			"email col_email",
		}).
		Where(Where().
			Bt("col1", "start1", "end1"),
		)
	
	query := Union_all().
		Select([]string{
			"col_id",
			"col_email",
		}).
		Union(query_union1).
		Union(query_union2).
		Group([]string{
			"grp1",
		}).
		Limit(0, 1)
	
	sql, _ := query.Compile()
	
	want :=
`SELECT col_id, col_email
FROM (
SELECT id col_id, email col_email
FROM .user
WHERE col1=? AND email>?
UNION ALL
SELECT id col_id, email col_email
FROM .group
WHERE col1 BETWEEN ? AND ?
) t
GROUP BY grp1
LIMIT 0,1`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`SELECT col_id, col_email
FROM (
SELECT id col_id, email col_email
FROM .user
WHERE col1=123 AND email>test2
UNION ALL
SELECT id col_id, email col_email
FROM .group
WHERE col1 BETWEEN start1 AND end1
) t
GROUP BY grp1
LIMIT 0,1`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_insert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_insert(b)
		run_insert_update_duplicate(b)
		run_insert_update_duplicate_operator(b)
		run_insert_update_duplicate_operator_fields(b)
		run_insert_update_duplicate_fields(b)
	}
}

func Test_insert(t *testing.T){
	t.Run("insert", func(t *testing.T){
		run_insert(t)
	})
	
	t.Run("insert update duplicate", func(t *testing.T){
		run_insert_update_duplicate(t)
	})
	
	t.Run("insert update duplicate operator", func(t *testing.T){
		run_insert_update_duplicate_operator(t)
	})
	
	t.Run("insert update duplicate operator fields", func(t *testing.T){
		run_insert_update_duplicate_operator_fields(t)
	})
	
	t.Run("insert update duplicate fields", func(t *testing.T){
		run_insert_update_duplicate_fields(t)
	})
}

func run_insert(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .user
SET time_login=123`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_insert_update_duplicate(tb testing.TB){
	query := Insert("user").
		Update_duplicate(nil).
		Fields(Map{
			"time_login":	123,
			"name":			"test",
		})
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .user
SET name=?, time_login=?
ON DUPLICATE KEY UPDATE name=?, time_login=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .user
SET name=test, time_login=123
ON DUPLICATE KEY UPDATE name=test, time_login=123`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_insert_update_duplicate_operator(tb testing.TB){
	query := Insert("user").
		Update_duplicate_operator(Fields().
			Add("balance", 12).
			Add("draft", -10),
			nil,
		)
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .user
SET balance=?, draft=?
ON DUPLICATE KEY UPDATE balance=balance+?, draft=draft+?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .user
SET balance=12, draft=-10
ON DUPLICATE KEY UPDATE balance=balance+12, draft=draft+-10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_insert_update_duplicate_operator_fields(tb testing.TB){
	query := Insert("user").
		Update_duplicate_operator(Fields().
			Add("balance", 12).
			Add("draft", -10),
			[]string{"draft"},
		)
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .user
SET balance=?, draft=?
ON DUPLICATE KEY UPDATE draft=draft+?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .user
SET balance=12, draft=-10
ON DUPLICATE KEY UPDATE draft=draft+-10`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_insert_update_duplicate_fields(tb testing.TB){
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
SET name=?, time_login=?
ON DUPLICATE KEY UPDATE time_login=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .user
SET name=test, time_login=123
ON DUPLICATE KEY UPDATE time_login=123`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_inserts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_inserts(b)
		run_inserts_update_duplicate(b)
		run_inserts_update_duplicate_fields(b)
	}
}

func Test_inserts(t *testing.T){
	t.Run("inserts", func(t *testing.T){
		run_inserts(t)
	})
	
	t.Run("inserts update duplicate", func(t *testing.T){
		run_inserts_update_duplicate(t)
	})
	
	t.Run("inserts update duplicate fields", func(t *testing.T){
		run_inserts_update_duplicate_fields(t)
	})
}

func run_inserts(tb testing.TB){
	query := Inserts("account")
	query.Fields(Map{
		"account_number":	123,
		"name":				"test1",
	})
	query.Fields(Map{
		"account_number":	456,
		"name":				"test2",
	})
	query.Fields(Map{
		"account_number":	789,
		"name":				"test3",
	})
	query.Fields(Map{
		"account_number":	101112,
		"name":				"test4",
	})
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .account (account_number, name)
VALUES (?,?),(?,?),(?,?),(?,?)`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .account (account_number, name)
VALUES (123,test1),(456,test2),(789,test3),(101112,test4)`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_inserts_update_duplicate(tb testing.TB){
	query := Inserts("account").
		Update_duplicate(nil)
	query.Fields(Map{
		"account_number":	123,
		"name":				"test1",
	})
	query.Fields(Map{
		"account_number":	456,
		"name":				"test2",
	})
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .account (account_number, name)
VALUES (?,?),(?,?)
ON DUPLICATE KEY UPDATE account_number=VALUES(account_number),name=VALUES(name)`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .account (account_number, name)
VALUES (123,test1),(456,test2)
ON DUPLICATE KEY UPDATE account_number=VALUES(account_number),name=VALUES(name)`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_inserts_update_duplicate_fields(tb testing.TB){
	query := Inserts("account").
		Update_duplicate([]string{
			"name",
		})
	query.Fields(Map{
		"account_number":	123,
		"name":				"test1",
	})
	query.Fields(Map{
		"account_number":	456,
		"name":				"test2",
	})
	
	sql, _ := query.Compile()
	
	want :=
`INSERT .account (account_number, name)
VALUES (?,?),(?,?)
ON DUPLICATE KEY UPDATE name=VALUES(name)`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`INSERT .account (account_number, name)
VALUES (123,test1),(456,test2)
ON DUPLICATE KEY UPDATE name=VALUES(name)`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_update(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_update_id_empty(b)
		run_update_id_set(b)
		run_update(b)
		run_update_operator(b)
	}
}

func Test_update(t *testing.T){
	t.Run("update id empty", func(t *testing.T){
		run_update_id_empty(t)
	})
	
	t.Run("update id set", func(t *testing.T){
		run_update_id_set(t)
	})
	
	t.Run("update", func(t *testing.T){
		run_update(t)
	})
	
	t.Run("update operator", func(t *testing.T){
		run_update_operator(t)
	})
}

func run_update_id_empty(tb testing.TB){
	query := Update_id("user", 0).
		Fields(Map{
			"time_login": 123,
		})
	
	sql, _ := query.Compile()
	
	want :=
`UPDATE .user
SET time_login=?
WHERE id=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`UPDATE .user
SET time_login=123
WHERE id=0`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_update_id_set(tb testing.TB){
	query := Update_id("user", 100).
		Fields(Map{
			"time_login": 123,
		})
	
	sql, _ := query.Compile()
	
	want :=
`UPDATE .user
SET time_login=?
WHERE id=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`UPDATE .user
SET time_login=123
WHERE id=100`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_update(tb testing.TB){
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
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`UPDATE .user
SET time_login=123`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_update_operator(tb testing.TB){
	query := Update_id("user", 123).
		Fields_operator(Fields().
			Add("balance", 12).
			Add("draft", -10),
		)
	
	sql, _ := query.Compile()
	
	want :=
`UPDATE .user
SET balance=balance+?, draft=draft+?
WHERE id=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`UPDATE .user
SET balance=balance+12, draft=draft+-10
WHERE id=123`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func Benchmark_delete(b *testing.B) {
	for i := 0; i < b.N; i++ {
		run_delete_id_empty(b)
		run_delete_id_set(b)
		run_delete(b)
	}
}

func Test_delete(t *testing.T){
	t.Run("delete id empty", func(t *testing.T){
		run_delete_id_empty(t)
	})
	
	t.Run("delete id set", func(t *testing.T){
		run_delete_id_set(t)
	})
	
	t.Run("delete", func(t *testing.T){
		run_delete(t)
	})
}

func run_delete_id_empty(tb testing.TB){
	query := Delete_id("user", 0)
	
	sql, _ := query.Compile()
	
	want :=
`DELETE FROM .user
WHERE id=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`DELETE FROM .user
WHERE id=0`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_delete_id_set(tb testing.TB){
	query := Delete_id("user", 100)
	
	sql, _ := query.Compile()
	
	want :=
`DELETE FROM .user
WHERE id=?`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`DELETE FROM .user
WHERE id=100`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}

func run_delete(tb testing.TB){
	query := Delete("user")
	
	sql, _ := query.Compile()
	
	want :=
`DELETE FROM .user`
	got := strings.TrimSpace(sql)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
	
	want =
`DELETE FROM .user`
	got = SQL_debug(query)
	if got != want {
		tb.Fatalf("SQL want:\n%s\nSQL got:\n%s", want, got)
	}
}