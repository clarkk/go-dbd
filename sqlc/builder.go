package sqlc

import (
	"fmt"
	"strings"
)

type (
	sbuilder struct {
		strings.Builder
	}
	
	sbuilder_audit struct {
		sb		*sbuilder
		name	string
		len		int
		cap		int
		grow	int
	}
)

//	Only grow if necessary
func (s *sbuilder) Alloc(n int){
	free	:= s.Cap() - s.Len()
	alloc	:= n - free
	if alloc > 0 {
		s.Grow(alloc)
	}
}

//	Debug
func Audit(sb *sbuilder, name string) *sbuilder_audit {
	base_len	:= sb.Len()
	base_cap	:= sb.Cap()
	
	free		:= base_cap - base_len
	
	fmt.Printf("### Base: %s\n\tLen: %d\n\tCap: %d\n\tFree: %d\n\n", name, base_len, base_cap, free)
	
	return &sbuilder_audit{
		sb:		sb,
		name:	name,
		len:	base_len,
		cap:	base_cap,
	}
}

func (a *sbuilder_audit) Grow(count int){
	a.grow = count
}

func (a *sbuilder_audit) Audit(){
	audit_len	:= a.sb.Len()
	audit_cap	:= a.sb.Cap()
	
	free		:= audit_cap - audit_len
	
	grow			:= audit_len - a.len
	cap_increase	:= audit_cap - a.cap
	
	fmt.Printf("### Audit: %s\n\tLen: %d\n\tCap: %d\n\tFree: %d\n", a.name, audit_len, audit_cap, free)
	fmt.Printf("Added/Grow: %d/%d (Cap increase: %d)\n%s\n", grow, a.grow, cap_increase, a.sb.String()[a.len:])
	
	grow_diff := grow - a.grow
	if grow_diff > 0 {
		fmt.Printf("\t\t\t\t\t\t\t\t\tGrow diff: %d\n", grow_diff)
	}
}