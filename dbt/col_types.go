package dbt

import (
	//"fmt"
)

type Col_typer interface {
	New() any
	Deref(any) any
}

type Col_type[T any] struct{}

func (v Col_type[T]) New() any {
	return new(T)
}

func (v Col_type[T]) Deref(p any) any {
	return *p.(*T)
}

/*type (
	
	
	Type_string 	string
	Type_int 		int
)

func (t *Type_string) Scan(value any) error {
    switch value := value.(type) {
    case []uint8:
        *t = Type_string(value)
    default:
        return fmt.Errorf("Invalid database type: %T %v", value, value)
    }
    return nil
}

func (t *Type_int) Scan(value any) error {
    switch value := value.(type) {
    case int64:
        *t = Type_int(value)
    default:
        return fmt.Errorf("Invalid database type: %T %v", value, value)
    }
    return nil
}*/