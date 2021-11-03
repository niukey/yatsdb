package label2db

type Metrics struct {
	Labels []DataID
	ID     uint64
}

type LabelEntry struct {
	Name     DataID
	Value    DataID
	MetricID uint64
}

type LabelTable struct {
	entries []LabelEntry
}

type DB struct {
	dataTable   *dataTable
	labelTables []LabelTable
}

type Interactor struct {
	db     *DB
	Prefix DataID
	i      int
}

func (iter *Interactor) Next() (string, error) {
	return "", nil
}

func (db *DB) New(prefix string) *Interactor {
	return nil
}
