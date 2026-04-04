package dbd

import "fmt"

type DSN struct {
	user 	string
	pass 	string
	db 		string
	charset string
}

func NewDSN(user, pass, db, charset string) DSN {
	return DSN{
		user,
		pass,
		db,
		charset,
	}
}

func (d DSN) Socket(socket string) string {
	return fmt.Sprintf("%s:%s@unix(%s)/%s?charset=%s",
		d.user,
		d.pass,
		socket,
		d.db,
		d.charset,
	)
}

func (d DSN) TCP(host string, port int) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		d.user,
		d.pass,
		host,
		port,
		d.db,
		d.charset,
	)
}