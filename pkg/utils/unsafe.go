package utils

import (
	"reflect"
	"unsafe"
)

func UnsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}

func UnsafeString(str *string, data []byte) {
	s := (*reflect.StringHeader)(unsafe.Pointer(str))
	s.Data = uintptr(unsafe.Pointer(&data[0]))
	s.Len = len(data)
}
