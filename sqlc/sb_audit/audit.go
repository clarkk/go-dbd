package sb_audit

import "fmt"

type (
	builder interface {
		Len() int
		Cap() int
		String() string
	}
	
	audit struct {
		name	string
		sb		builder
		len		int
		cap		int
		grow	int
	}
)

func Base(sb builder, name string) *audit {
	base_len	:= sb.Len()
	base_cap	:= sb.Cap()
	
	free		:= base_cap - base_len
	
	fmt.Printf("### Base: %s\n\tLen: %d\n\tCap: %d\n\tFree: %d\n\n", name, base_len, base_cap, free)
	
	return &audit{
		name:	name,
		sb:		sb,
		len:	base_len,
		cap:	base_cap,
	}
}

func (a *audit) Grow(count int){
	a.grow = count
}

func (a *audit) Audit(){
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