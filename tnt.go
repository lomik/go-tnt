package tnt

type Field []byte
type Tuple []Field

const requestTypeCall = 22
const requestTypeDelete = 21
const requestTypeInsert = 13
const requestTypeSelect = 17
const requestTypeUpdate = 19

type Query interface {
	Pack() []byte
}

type Select struct {
	// Scalar
	// This request is looking for one single record
	Value Field

	// List of scalars
	// This request is looking for several records using single-valued index
	// Ex: select(space_no, index_no, [1, 2, 3])
	// Transform a list of scalar values to a list of tuples
	Values []Field

	// List of tuples
	// This request is looking for serveral records using composite index
	Tuples []Tuple

	Space  uint32
	Index  uint32
	Limit  uint32 // 0x0 == 0xffffffff
	Offset uint32
}

type Insert struct {
	Tuple       Tuple
	Space       uint32
	ReturnTuple bool
}
