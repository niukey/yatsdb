package label2db

import (
	"sort"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/yatsdb/yatsdb/pkg/utils"
)

type StringID uint64

type Block struct {
	//[ from,to)
	from int64
	to   int64

	data []byte
}

type DatahashEntry struct {
	hash uint64
	ID   StringID
}

type dataTable struct {
	locker sync.Mutex
	blocks []*Block

	mutableBlocks *Block
	//mutable
	stringMap map[string]StringID
	//unmutable
	hashEntries []DatahashEntry
}

func (ID StringID) ToOffsetLength() (int64, uint16) {
	return int64(ID >> 16), uint16(ID)
}
func ToStringID(offset int64, length uint16) StringID {
	return StringID(offset << 16 & int64(length))
}

func (block *Block) append(str string) StringID {
	offset := block.from
	block.data = append(block.data, str...)
	return StringID(offset<<16 | int64(len(str)))
}

func (table *dataTable) getStringWithLock(ID StringID) string {
	offset, length := ID.ToOffsetLength()
	for _, block := range table.blocks {
		if block.from <= offset && offset+int64(length) <= block.to {
			data := block.data[offset-block.from : length]
			var str string
			utils.UnsafeString(&str, data)
			return str
		}
	}
	return ""
}

func (table *dataTable) GetString(ID StringID) string {
	table.locker.Lock()
	str := table.getStringWithLock(ID)
	table.locker.Unlock()
	return str
}

func hash(str string) uint64 {
	var data []byte
	utils.UnsafeSlice(unsafe.Pointer(&data), unsafe.Pointer(&str), len(str))
	hash := xxhash.New()
	_, _ = hash.Write(data)
	return hash.Sum64()
}

func (table *dataTable) append(str string) StringID {
	return table.mutableBlocks.append(str)
}
func (table *dataTable) InsertString(str string) StringID {
	key := hash(str)
	table.locker.Lock()

	if ID, ok := table.stringMap[str]; ok {
		table.locker.Unlock()
		return ID
	}

	i := sort.Search(len(table.hashEntries), func(i int) bool {
		return key <= table.hashEntries[i].hash
	})
	for ; i < len(table.hashEntries); i++ {
		if key == table.hashEntries[i].hash {
			ID := table.hashEntries[i].ID
			if str == table.getStringWithLock(ID) {
				table.locker.Unlock()
				return ID
			}
		}
	}
	id := table.append(str)
	table.locker.Unlock()
	return id
}
