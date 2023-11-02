package dbd

import (
	"testing"
	c "github.com/clarkk/go-dbd/dbc"
	t "github.com/clarkk/go-dbd/dbt"
	v "github.com/clarkk/go-dbd/dbv"
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

var App = c.NewCollection().Apply(v.NewView(
	Block,
	true,
)).Apply(v.NewView(
	Client,
	false,
))

func Test_Query(t *testing.T){
	
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