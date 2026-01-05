package sqlc

import "strings"

type sbuilder struct {
	strings.Builder
}

//	Only grow if necessary
func (s *sbuilder) Alloc(n int){
	free	:= s.Cap() - s.Len()
	alloc	:= n - free
	if alloc > 0 {
		s.Grow(alloc)
	}
}