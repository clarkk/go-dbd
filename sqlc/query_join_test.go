package sqlc

/*
	Test
	# go test . -v
	
	Benchark
	# go test -benchmem -bench=. -run=^$
*/

import (
	"slices"
	"reflect"
	"testing"
)

type query_join_test struct {
	query_join
	aliases		alias_collect
}

func Query_join_test(table string) *query_join_test {
	return &query_join_test{
		query_join: query_join{
			query: query{
				table: table,
			},
			optimize_joins: true,
		},
		aliases: alias_collect{},
	}
}

func (q *query_join_test) aliases_apply(alias string){
	q.aliases.apply(alias)
}

func (q *query_join_test) resolve_alias_join_dependencies() error {
	return q.query_join.resolve_alias_join_dependencies(q.aliases)
}

func (q *query_join_test) aliases_sorted() []string {
	return q.aliases.sorted()
}

func (q *query_join_test) compile_optimize_joins() []join {
	return q.query_join.compile_optimize_joins(q.aliases)
}

func Test_optimize_joins(t *testing.T){
	t.Run("optimize_joins", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("bank", "b", "id", "i.bank_id")
		q.left_join("module", "m", "id", "i.accounting_module_id")
		q.left_join("invoice", "i", "id", "invoice_id")
		
		q.aliases_apply("b.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"b", "i"}
		got_alias	:= q.aliases_sorted()
		if !slices.Equal(want_alias, got_alias) {
			t.Fatalf("Alias want:\n%s\nAlias got:\n%s", want_alias, got_alias)
		}
		
		want_joins := []join{{
			mode:			join_left,
			table:			"invoice",
			t:				"i",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"invoice_id",
			}},
			depth:			0,
		},{
			mode:			join_left,
			table:			"bank",
			t:				"b",
			join_t:			[]string{"i"},
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"i.bank_id",
			}},
			depth:			1,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
	
	t.Run("independent_joins", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("bank", "b", "id", "bank_id")
		q.left_join("module", "m", "id", "module_id")
		
		q.aliases_apply("b.name")
		q.aliases_apply("m.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"b", "m"}
		got_alias	:= q.aliases_sorted()
		if !slices.Equal(want_alias, got_alias) {
			t.Fatalf("Alias want:\n%s\nAlias got:\n%s", want_alias, got_alias)
		}
		
		want_joins := []join{{
			mode:			join_left,
			table:			"bank",
			t:				"b",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"bank_id",
			}},
			depth:			0,
		},{
			mode:			join_left,
			table:			"module",
			t:				"m",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"module_id",
			}},
			depth:			0,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
	
	t.Run("multiple_dependencies", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("bank", "b", "id", "bank_id")
		q.left_join("module", "m", "id", "module_id")
		
		q.left_join_multi("invoice", "i", Join_conditions{{
			Field:			"bank_id",
			Field_foreign:	"b.id",
		},{
			Field:			"module_id",
			Field_foreign:	"m.id",
		}})
		
		q.aliases_apply("i.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"b", "i", "m"}
		got_alias	:= q.aliases_sorted()
		if !slices.Equal(want_alias, got_alias) {
			t.Fatalf("Alias want:\n%s\nAlias got:\n%s", want_alias, got_alias)
		}
		
		want_joins := []join{{
			mode:			join_left,
			table:			"bank",
			t:				"b",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"bank_id",
			}},
			depth:			0,
		},{
			mode:			join_left,
			table:			"module",
			t:				"m",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"module_id",
			}},
			depth:			0,
		},{
			mode:			join_left,
			table:			"invoice",
			t:				"i",
			join_t:			[]string{"b", "m"},
			on: Join_conditions{{
				Field:			"bank_id",
				Field_foreign:	"b.id",
			},{
				Field:			"module_id",
				Field_foreign:	"m.id",
			}},
			depth:			1,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
}