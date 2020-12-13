package configuration

import "errors"

var (
	// Default errors

	ErrEmptyKey       = errors.New("Empty configuration key")
	ErrKeyNotFound    = errors.New("Specified configuration key not found")
	ErrInvalidBool    = errors.New("Invalid value for boolean field")
	ErrInvalidInt     = errors.New("Invalid value for int field")
	ErrInvalidInt64   = errors.New("Invalid value for int64 field")
	ErrInvalidUint    = errors.New("Invalid value for uint field")
	ErrInvalidUint16  = errors.New("Invalid value for uint16 field")
	ErrInvalidUint64  = errors.New("Invalid value for uint64 field")
	ErrInvalidFloat64 = errors.New("Invalid value for float64 field")
	ErrStructPtrReq   = errors.New("pointer to struct should be provided")
)
