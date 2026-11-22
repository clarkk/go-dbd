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
	
	t.Run("dependency_chain", func(t *testing.T){
		q := Query_join_test("table")
		
		//	Register joins in reverse dependency order
		q.left_join("bank", "b", "id", "a.bank_id")
		q.left_join("account", "a", "id", "i.account_id")
		q.left_join("invoice", "i", "id", "invoice_id")
		
		q.aliases_apply("b.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"a", "b", "i"}
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
			table:			"account",
			t:				"a",
			join_t:			[]string{"i"},
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"i.account_id",
			}},
			depth:			1,
		},{
			mode:			join_left,
			table:			"bank",
			t:				"b",
			join_t:			[]string{"a"},
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"a.bank_id",
			}},
			depth:			2,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
	
	t.Run("circular_dependency", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("bank", "b", "id", "a.bank_id")
		q.left_join("account", "a", "id", "b.account_id")
		
		q.aliases_apply("b.name")
		
		if err := q.resolve_alias_join_dependencies(); err == nil {
			t.Fatal("expected an error for circular dependency b -> a -> b")
		}
	})
	
	t.Run("unused_inner_join_is_removed", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("account", "a", "id", "account_id")
		q.inner_join("zone", "z", "id", "zone_id")
		
		q.aliases_apply("a.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"a"}
		got_alias	:= q.aliases_sorted()
		if !slices.Equal(want_alias, got_alias) {
			t.Fatalf("Alias want:\n%s\nAlias got:\n%s", want_alias, got_alias)
		}
		
		want_joins := []join{{
			mode:			join_left,
			table:			"account",
			t:				"a",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"account_id",
			}},
			depth:			0,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
	
	t.Run("inner_join_first_at_same_depth_force", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("account", "a", "id", "account_id")
		q.inner_join_force("zone", "z", "id", "zone_id")
		
		q.aliases_apply("a.name")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"a", "z"}
		got_alias	:= q.aliases_sorted()
		if !slices.Equal(want_alias, got_alias) {
			t.Fatalf("Alias want:\n%s\nAlias got:\n%s", want_alias, got_alias)
		}
		
		want_joins := []join{{
			mode:			join_inner,
			table:			"zone",
			t:				"z",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"zone_id",
			}},
			depth:			0,
			force:			true,
		},{
			mode:			join_left,
			table:			"account",
			t:				"a",
			join_t:			nil,
			on: Join_conditions{{
				Field:			"id",
				Field_foreign:	"account_id",
			}},
			depth:			0,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
	
	t.Run("multiple_dependencies_use_max_depth", func(t *testing.T){
		q := Query_join_test("table")
		
		q.left_join("bank", "b", "id", "bank_id")
		q.left_join("invoice", "i", "bank_id", "b.id")
		q.left_join_multi("payment", "p", Join_conditions{{
			Field:			"invoice_id",
			Field_foreign:	"i.id",
		}, {
			Field:			"bank_id",
			Field_foreign:	"b.id",
		}})
		
		q.aliases_apply("p.amount")
		
		if err := q.resolve_alias_join_dependencies(); err != nil {
			t.Fatal(err)
		}
		
		want_alias	:= []string{"b", "i", "p"}
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
			force:			false,
		},{
			mode:			join_left,
			table:			"invoice",
			t:				"i",
			join_t:			[]string{"b"},
			on: Join_conditions{{
				Field:			"bank_id",
				Field_foreign:	"b.id",
			}},
			depth:			1,
		},{
			mode:			join_left,
			table:			"payment",
			t:				"p",
			join_t:			[]string{"i", "b"},
			on: Join_conditions{{
				Field:			"invoice_id",
				Field_foreign:	"i.id",
			},{
				Field:			"bank_id",
				Field_foreign:	"b.id",
			}},
			depth:			2,
		}}
		got_joins := q.compile_optimize_joins()
		if !reflect.DeepEqual(want_joins, got_joins) {
			t.Fatalf("Joins want:\n%v\nJoins got:\n%v", want_joins, got_joins)
		}
	})
}