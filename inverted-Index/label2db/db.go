package label2db

type DB struct {
	mtables       *MemTable
	flushingTable *MemTable
	SegmentTables []SegmetTable
}
