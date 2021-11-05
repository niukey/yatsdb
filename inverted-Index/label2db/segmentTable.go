package label2db

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/yatsdb/yatsdb/pkg/utils"
)

type LabelValueNode struct {
	LeftChild  int64
	RightChild int64
	ValueLen   uint16
	IDSize     uint32
}

type LabelSetNode struct {
	LeftChild  int64
	RightChild int64
	Start      int64
	End        int64
	//Label      []byte
	LabelLen uint16
}

type SegmetTable struct {
	data   []byte
	mfile  *fileutil.MmapFile
	header SegmentHeaderV0
}

type SegmentHeaderV0 struct {
	CreateMills    int64
	LabelSetOffset int64
	MergeCount     int32
	IDCount        int32
}

func openSegment(filepath string) (*SegmetTable, error) {
	mfile, err := fileutil.OpenMmapFile(filepath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	data := mfile.Bytes()
	if len(data) == 0 {
		return nil, errors.New("empty segment")
	}
	var header SegmentHeaderV0
	if data[0] == '1' {
		header = *(*SegmentHeaderV0)(unsafe.Pointer(&data[1]))
	} else {
		return nil, errors.New("unknown segment header version")
	}
	return &SegmetTable{
		data:   data,
		mfile:  mfile,
		header: header,
	}, nil
}

const LabelSetNodeSize = uint16(unsafe.Sizeof(LabelSetNode{}))
const LabelValueNodeSize = uint16(unsafe.Sizeof(LabelValueNode{}))
const emptyNode = int64(0)

var (
	errNoFind = errors.New("no find error")
)

func (lSet *LabelSetNode) GetLabel(data []byte) []byte {
	return data[LabelSetNodeSize : LabelSetNodeSize+lSet.LabelLen]
}

func (lSet *LabelValueNode) GetValue(data []byte) []byte {
	return data[LabelValueNodeSize : LabelValueNodeSize+lSet.ValueLen]
}

func (lVal *LabelValueNode) Size() int64 {
	return int64(LabelValueNodeSize + lVal.ValueLen + uint16(lVal.IDSize*8))
}
func (lVal *LabelValueNode) GetIDs(data []byte) []uint64 {
	offset := uint32(LabelValueNodeSize + lVal.ValueLen)
	var IDs []uint64
	utils.UnsafeSlice(unsafe.Pointer(&IDs), unsafe.Pointer(&data[offset]), int(lVal.IDSize))
	return IDs
}

func decodeLabelValueNode(data []byte) *LabelValueNode {
	return (*LabelValueNode)(unsafe.Pointer(&data[0]))
}

func decodeLabelSetNode(data []byte) *LabelSetNode {
	return (*LabelSetNode)(unsafe.Pointer(&data[0]))
}

func (segment *SegmetTable) GetValueNodeLabel(node *LabelValueNode) []byte {
	return node.GetValue(segment.data)
}
func (segment *SegmetTable) GetValueNodeIDs(node *LabelValueNode) []uint64 {
	return node.GetIDs(segment.data)
}

func (segment *SegmetTable) GetLabelSetNode(labelKey []byte) (*LabelSetNode, error) {
	return segment.findLabelSetNode(segment.header.LabelSetOffset, labelKey)
}
func (segment *SegmetTable) findLabelSetNode(offset int64, labelKey []byte) (*LabelSetNode, error) {
	data := segment.data[offset:]
	node := decodeLabelSetNode(data)
	label := node.GetLabel(data)
	if c := bytes.Compare(label, labelKey); c == 0 {
		return node, nil
	} else if c < 0 {
		if node.LeftChild == emptyNode {
			return nil, errNoFind
		}
		return segment.findLabelSetNode(node.LeftChild, labelKey)
	} else {
		if node.RightChild == emptyNode {
			return nil, errNoFind
		}
		return segment.findLabelSetNode(node.RightChild, labelKey)
	}
}

func (segment *SegmetTable) findLabelValueNode(offset int64, valueKey []byte) (*LabelValueNode, error) {
	data := segment.data[offset:]
	node := decodeLabelValueNode(data)
	value := node.GetValue(data)
	if c := bytes.Compare(value, valueKey); c == 0 {
		return node, nil
	} else if c < 0 {
		if node.LeftChild == emptyNode {
			return nil, errNoFind
		}
		return segment.findLabelValueNode(node.LeftChild, valueKey)
	} else {
		if node.RightChild == emptyNode {
			return nil, errNoFind
		}
		return segment.findLabelValueNode(node.RightChild, valueKey)
	}
}

func (segment *SegmetTable) LabelValueNodeIteractor(node *LabelSetNode) *LabelValueNodeIteractor {
	return &LabelValueNodeIteractor{
		segment: segment,
		node:    node,
		offset:  node.Start,
	}
}

type LabelValueNodeIteractor struct {
	segment *SegmetTable
	node    *LabelSetNode
	offset  int64
}

func (iter *LabelValueNodeIteractor) Next() (*LabelValueNode, error) {
	if iter.offset >= iter.node.End {
		return nil, io.EOF
	}
	node := decodeLabelValueNode(iter.segment.data[iter.offset:])
	iter.offset += node.Size()
	return node, nil
}
