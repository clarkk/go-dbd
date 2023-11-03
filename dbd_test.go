package dbd

import (
	"testing"
	"github.com/clarkk/go-dbd/dbq"
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
	g 			*dbq.Query_get
	
	got_code 	dbq.Error_code
	want_code 	dbq.Error_code
	
	want 		string
	got 		string
	
	block_private 	= dbv.NewView(Block, false)
	block_public 	= dbv.NewView(Block, true)
)

func Test_errors(t *testing.T){
	//	-------------------------------------------------------------------------
	//	Table private
	//	-------------------------------------------------------------------------
	want_code = 										dbq.ERR_CODE_PRIVATE
	
	g = dbq.NewQuery_get("block", block_private);
	g.Public()
	g.Select(dbq.Select{
		"id",
	})
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Select empty
	//	-------------------------------------------------------------------------
	want_code = 										dbq.ERR_CODE_SELECT_EMPTY
	
	g = dbq.NewQuery_get("block", block_private);
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Fields invalid
	//	-------------------------------------------------------------------------
	want_code = 										dbq.ERR_CODE_FIELDS_INVALID
	
	//	Invalid select
	g = dbq.NewQuery_get("block", block_private);
	g.Select(dbq.Select{
		"test",
	})
	write_get(t, g, want_code)
	
	//	Invalid where
	g = dbq.NewQuery_get("block", block_private);
	g.Select(dbq.Select{
		"id",
	})
	g.Where(dbq.Where{
		"test": "",
	})
	write_get(t, g, want_code)
	
	//	-------------------------------------------------------------------------
	//	Where values invalid
	//	-------------------------------------------------------------------------
	want_code = 										dbq.ERR_CODE_WHERE_VALUES
	
	//	Invalid where
	g = dbq.NewQuery_get("block", block_private);
	g.Select(dbq.Select{
		"id",
	})
	g.Where(dbq.Where{
		"name": []string{
			"noget",
		},
	})
	write_get(t, g, want_code)
}

func Test_query(t *testing.T){
	want_code =											dbq.ERR_CODE_SUCCESS
	
	g = dbq.NewQuery_get("block", block_private);
	g.Select(dbq.Select{
		"id",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT id
FROM .block`)
	
	g = dbq.NewQuery_get("block", block_private);
	g.Select(dbq.Select{
		"id",
		"is_suspended",
	})
	write_get(t, g, want_code)
	sql_get(t, g, `SELECT a.id,b.is_suspended
FROM .block a
LEFT JOIN .client b ON a.client_id=b.id`)
}

func sql_get(t *testing.T, g *dbq.Query_get, want string){
	got = g.SQL()
	check_query(t, got, want)
}

func write_get(t *testing.T, g *dbq.Query_get, want dbq.Error_code){
	got_code, _ = g.Write()
	check_code(t, got_code, want_code)
}

func check_code(t *testing.T, got dbq.Error_code, want dbq.Error_code){
	if want != got {
		t.Errorf("\ngot: %d\nwant: %d", got, want)
	}
}

func check_query(t *testing.T, got string, want string){
	if want != got {
		t.Errorf("\ngot:\n%s\nwant:\n%s", got, want)
	}
}