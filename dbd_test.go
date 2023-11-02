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

func Test_Query(t *testing.T){
	var (
		qg 		*dbq.Query_get
		code 	dbq.Error_code
	)
	qg = dbq.NewQuery_get("block", dbv.NewView(
		Block,
		true,
	));
	qg.Select(
		dbq.Select{
			"id",
		},
	)
	code, _ = qg.Write()
	t.Log("code", code)
	
	/*key_bytes, pub_bytes 	:= Generate_RSA(BITS)
	key_len 				:= len(key_bytes)
	if key_len == 0 {
		t.Errorf("private key %d", key_len)
	}
	pub_len 				:= len(pub_bytes)
	if pub_len == 0 {
		t.Errorf("private key %d", pub_len)
	}
	
	if !Verify_RSA(key_bytes, pub_bytes) {
		t.Error("private and public key could not be verified")
	}
	
	msg := "Hello world!"
	
	ciphertext := Encrypt_public(msg, pub_bytes)
	if Decrypt_private(ciphertext, key_bytes) != msg {
		t.Error("rsa encryption failed")
	}
	
	cipher_base64 := Encrypt_public_base64(msg, pub_bytes)
	if Decrypt_private_base64(cipher_base64, key_bytes) != msg {
		t.Error("rsa encryption base64 failed")
	}
	
	signature 	:= Sign(msg, key_bytes)
	if !Verify(msg, signature, pub_bytes) {
		t.Error("signature could not be verified")
	}
	
	sig_base64 	:= Sign_base64(msg, key_bytes)
	if !Verify_base64(msg, sig_base64, pub_bytes) {
		t.Error("signature could not be verified with base64 encoding")
	}*/
}