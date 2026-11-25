package dbd

import (
	"fmt"
	"net"
)

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
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s",
		d.user,
		d.pass,
		addr,
		d.db,
		d.charset,
	)
}