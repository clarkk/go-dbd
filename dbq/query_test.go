package dbq

import (
	"testing"
	t "github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
)

const (
	block 	= "block"
	client 	= "client"
)

var Block = t.NewTable(
	block,
	t.Fields{
		"id":			t.Field{block, "id"},
		"client_id":	t.Field{block, "client_id"},
		"is_suspended":	t.Field{client, "is_suspended"},
		"name":			t.Field{block, "name"},
		"renamed":		t.Field{block, "name"},
	},
	t.Joins{
		client:			t.Join{t.LEFT_JOIN, "client_id", "id"},
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
		"id":			t.Field{client, "id"},
		"is_suspended":	t.Field{client, "is_suspended"},
		"time_created":	t.Field{client, "time_created"},
		"timeout":		t.Field{client, "timeout"},
		"lang":			t.Field{client, "lang"},
	},
	t.Joins{},
	t.Get{},
	t.Put{},
)

var (
	g 			*Query_get
	
	got_code 	Error_code
	want_code 	Error_code
	
	want 		string
	got 		string
	
	block_private 	= dbv.NewView(Block, false)
	block_public 	= dbv.NewView(Block, true)
)

func Test_errors(t *testing.T){
	//	-------------------------------------------------------------------------
	//	Table private
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_PRIVATE
	
	g = Get("block", block_private);
	g.Public()
	g.Select(Select{
		"id",
	})
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Select empty
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_SELECT_EMPTY
	
	g = Get("block", block_private);
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Fields invalid
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_FIELDS_INVALID
	
	//	Invalid select
	g = Get("block", block_private);
	g.Select(Select{
		"test",
	})
	write_get(t, g, want_code)
	
	//	Invalid select public
	g = Get("block", block_public);
	g.Public()
	g.Select(Select{
		"client_id",
	})
	write_get(t, g, want_code)
	
	//	Invalid where
	g = Get("block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"test": "",
	})
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Where values invalid
	//	-------------------------------------------------------------------------
	want_code = 										ERR_CODE_WHERE_VALUES
	
	//	Invalid where
	g = Get("block", block_private);
	g.Select(Select{
		"id",
	})
	g.Where(Where{
		"name": []string{
			"test",
		},
	})
	write_get(t, g, want_code)
}

func Test_query(t *testing.T){
	want_code =											ERR_CODE_SUCCESS
	
	g = Get("block", block_private);
	g.Select(Select{
		"id",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT id
FROM .block`)
	
	//	Select function
	g = Get("block", block_private);
	g.Select(Select{
		"count|id",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT count(id)
FROM .block`)
	
	//	Select "field as"
	g = Get("block", block_private);
	g.Select(Select{
		"id=new_id",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT id new_id
FROM .block`)
	
	//	Select function with "field as"
	g = Get("block", block_private);
	g.Select(Select{
		"count|id=new_id",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT count(id) new_id
FROM .block`)
	
	//	Join
	g = Get("block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT a.id,b.is_suspended
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`)
	
	//	Renamed field in table map
	g = Get("block", block_private);
	g.Select(Select{
		"id",
		"renamed",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT id,name renamed
FROM .block`)
	
	//	Renamed field in table map with join
	g = Get("block", block_private);
	g.Select(Select{
		"id",
		"is_suspended",
		"renamed",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT a.id,b.is_suspended,a.name renamed
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`)
	
	//	Renamed field in table map with join and "field as"
	g = Get("block", block_private);
	g.Select(Select{
		"id",
		"is_suspended=new_suspend",
		"renamed=new_name",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT a.id,b.is_suspended new_suspend,a.name new_name
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`)
	
	//	Renamed field in table map with join, "field as" and function
	g = Get("block", block_private);
	g.Select(Select{
		"id",
		"count|is_suspended=new_suspend",
		"sha1|renamed=new_name",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT a.id,count(b.is_suspended) new_suspend,sha1(a.name) new_name
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`)
}

func sql_get(t *testing.T, g *Query_get, want string){
	got = g.SQL()
	check_query(t, got, want)
}

func write_get(t *testing.T, g *Query_get, want Error_code){
	got_code, _ = g.Write()
	check_code(t, got_code, want_code)
}

func check_code(t *testing.T, got Error_code, want Error_code){
	if want != got {
		t.Errorf("\ngot: %d\nwant: %d", got, want)
	}
}

func check_query(t *testing.T, got string, want string){
	if want != got {
		t.Errorf("\ngot:\n%s\n\nwant:\n%s", got, want)
	}
}