package label2db

import (
	"testing"
	"unsafe"

	"github.com/yatsdb/yatsdb/pkg/utils"
	"gopkg.in/stretchr/testify.v1/assert"
)

func TestStringTable_GetString(t *testing.T) {
	var val = 0xfffffff0
	var data []byte
	var str string

	utils.UnsafeSlice(unsafe.Pointer(&data), unsafe.Pointer(&val), 4)
	utils.UnsafeString(&str, data)

	assert.Equal(t, []byte(str), data)

	var val2 = *(*int)(unsafe.Pointer(&data[0]))

	assert.Equal(t, val, val2)

	var id DataID

	var offset = uint64(1024)
	var length = uint16(32)

	id = DataID(offset<<16 | uint64(length))

	var offset2 = uint64(id >> 16)
	var length2 = uint16(id)

	assert.Equal(t, offset, offset2)
	assert.Equal(t, length, length2)
}
