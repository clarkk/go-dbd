package dbq

import (
	"fmt"
	"strconv"
	"testing"
	"context"
	"reflect"
	t "github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
)

const (
	block 		= "block"
	block_range = "block_range"
	client 		= "client"
)

var Block = t.NewTable(
	block,
	t.Fields{
		"id":				t.Field{block, "id"},
		"client_id":		t.Field{block, "client_id"},
		"is_suspended":		t.Field{client, "is_suspended"},
		"name":				t.Field{block, "name"},
		"renamed":			t.Field{block, "name"},
		"range_invoice":	t.Field{block_range, "invoice"},
	},
	t.Joins{
		block_range:		t.Join{t.INNER_JOIN, "id", "block_id"},
		client:				t.Join{t.LEFT_JOIN, "client_id", "id"},
	},
	t.Get{
		"id",
		"is_suspended",
		"name",
	},
	t.Put{
		//"name": "",
	},
)

var Client = t.NewTable(
	client,
	t.Fields{
		"id":				t.Field{client, "id"},
		"is_suspended":		t.Field{client, "is_suspended"},
		"time_created":		t.Field{client, "time_created"},
		"timeout":			t.Field{client, "timeout"},
		"lang":				t.Field{client, "lang"},
	},
	t.Joins{},
	t.Get{},
	t.Put{},
)

var (
	ctx 			= context.Background()
	
	g 				*Query_get
	
	got_code 		Error_code
	want_code 		Error_code
	
	want 			string
	got 			string
	
	block_private 	= dbv.NewView(Block, false)
	block_public 	= dbv.NewView(Block, true)
)

func Test_errors(t *testing.T){
	//	-------------------------------------------------------------------------
	//	Table private
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_PRIVATE
	
	g = Get(ctx, "block", block_private);
	g.Public()
	g.Select(Select{
		"id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Select empty
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_SELECT_EMPTY
	
	g = Get(ctx, "block", block_private);
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Fields invalid
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_FIELDS_INVALID
	
	//	Invalid select
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"test",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	Invalid select public
	g = Get(ctx, "block", block_public);
	g.Public()
	g.Select(Select{
		"client_id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	Invalid where
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"test": "test",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Where value
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_WHERE_VALUE
	
	//	Invalid where between value
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id bt": Where_op{
			1,2,3,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	Invalid where between value
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id bt": Where_op{
			3,1,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Where operator
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_WHERE_OPERATOR
	
	//	Invalid where operator
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"name ?": "",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Where values invalid
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_LIMIT_VALUE
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Limit(Limit{
		1,2,3,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Where values invalid
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_LIMIT_VALUE
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Limit(Limit{
		1,2,3,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Lock selected by id
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_SELECT_LOCK_ID
	
	g = Get(ctx, "block", block_private);
	g.Read_lock()
	g.Select(Select{
		"id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	g = Get(ctx, "block", block_private);
	g.Read_lock()
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id !": 123,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	
	//	-------------------------------------------------------------------------
	//	Limit value
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_LIMIT_VALUE
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Limit(Limit{
		0,10,2,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
}

func Test_query_select(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block`); err != "" {
		t.Errorf(err)
	}
	
	//	Select function
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"count|id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT count(id)
FROM .block`); err != "" {
		t.Errorf(err)
	}
	
	//	Select "field as"
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id=new_id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id new_id
FROM .block`); err != "" {
		t.Errorf(err)
	}
	
	//	Select function with "field as"
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"count|id=new_id",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT count(id) new_id
FROM .block`); err != "" {
		t.Errorf(err)
	}
	
	//	Join
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.is_suspended
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`); err != "" {
		t.Errorf(err)
	}
	
	//	Renamed field in table map
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"renamed",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id,name renamed
FROM .block`); err != "" {
		t.Errorf(err)
	}
	
	//	Renamed field in table map with join
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
		"renamed",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.is_suspended,a.name renamed
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`); err != "" {
		t.Errorf(err)
	}
	
	//	Renamed field in table map with join and "field as"
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"is_suspended=new_suspend",
		"renamed=new_name",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.is_suspended new_suspend,a.name new_name
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`); err != "" {
		t.Errorf(err)
	}
	
	//	Renamed field in table map with join, "field as" and function
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"count|is_suspended=new_suspend",
		"sha1|renamed=new_name",
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,count(b.is_suspended) new_suspend,sha1(a.name) new_name
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`); err != "" {
		t.Errorf(err)
	}
}

func Test_query_read_lock(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	g = Get(ctx, "block", block_private);
	g.Read_lock()
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id": 123,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id=?
FOR UPDATE`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"123",
	}); err != "" {
		t.Errorf(err)
	}
}

func Test_query_limit(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Limit(Limit{
		0,10,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
LIMIT 0,10`); err != "" {
		t.Errorf(err)
	}
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Limit(Limit{
		10,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
LIMIT 10`); err != "" {
		t.Errorf(err)
	}
}

func Test_query_select_where(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	//	Not equal
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id !": 43,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id!=?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"43",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Function
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"sha1|id": 43,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE sha1(id)=?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"43",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Not equal with join
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
	})
	g.Where(Where{
		"id": 63,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.is_suspended
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id
WHERE a.id=?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"63",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Greater than
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id >": 123,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id>=?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"123",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Less than
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id <": 154,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id<=?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"154",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Where in
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id in": Where_op{
			1,2,3,87,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id IN (?)`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"1,2,3,87",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Where in with join
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
	})
	g.Where(Where{
		"id in": Where_op{
			45,587,4,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.is_suspended
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id
WHERE a.id IN (?)`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"45,587,4",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Where not in
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id !in": Where_op{
			53,73,72,5474,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id NOT IN (?)`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"53,73,72,5474",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Where between
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id bt": Where_op{
			41,87,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id BETWEEN ? AND ?`); err != "" {
		t.Errorf(err)
	}
	if err := sql_values_get(t, g, []string{
		"41",
		"87",
	}); err != "" {
		t.Errorf(err)
	}
	
	//	Where not between
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"id !bt": Where_op{
			1,2,
		},
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT id
FROM .block
WHERE id NOT BETWEEN ? AND ?`); err != "" {
		t.Errorf(err)
	}
}

func Test_query_count(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	g = Get(ctx, "block", block_private);
	g.Select(Select{
		"id",
		"range_invoice",
		"is_suspended",
	})
	g.Limit(Limit{
		10,
	})
	if err := write_get(t, g, want_code); err != "" {
		t.Errorf(err)
	}
	if err := sql_get(t, g, `SELECT a.id,b.invoice range_invoice,c.is_suspended
FROM .block a
INNER JOIN .block_range b ON a.id=b.block_id
LEFT JOIN .client c ON a.client_id=c.id
LIMIT 10`); err != "" {
		t.Errorf(err)
	}
	
	g.Count()
	if err := sql_get(t, g, `SELECT count(*)
FROM .block a
INNER JOIN .block_range b ON a.id=b.block_id
LEFT JOIN .client c ON a.client_id=c.id`); err != "" {
		t.Errorf(err)
	}
}

func sql_values_get(t *testing.T, g *Query_get, want []string) string {
	if !reflect.DeepEqual(interface_string(g.sql_values), want) {
		return fmt.Sprintf("\ngot: %v\nwant: %v", g.sql_values, want)
	}
	return ""
}

func sql_get(t *testing.T, g *Query_get, want string) string {
	return check_query(t, g.sql, want)
}

func write_get(t *testing.T, g *Query_get, want Error_code) string {
	got_code, _ = g.prepare_select()
	return check_code(t, got_code, want_code)
}

func check_code(t *testing.T, got Error_code, want Error_code) string {
	if want != got {
		return fmt.Sprintf("\ngot: %d\nwant: %d", got, want)
	}
	return ""
}

func check_query(t *testing.T, got string, want string) string {
	if want != got {
		return fmt.Sprintf("\ngot:\n%s\n\nwant:\n%s", got, want)
	}
	return ""
}

func interface_string(a []any) []string {
	b := make([]string, len(a))
	for i := range a {
		switch value := a[i].(type) {
		case string:
			b[i] = value
		case int:
			b[i] = strconv.Itoa(value)
		}
	}
	return b
}