package tnt

import (
	"fmt"
	"sync/atomic"
)

func (conn *Connection) MemGet(key string) ([]byte, error) {
	req := &Select{
		Value: []byte(key),
		Space: conn.memcacheSpace,
	}

	res, err := conn.Execute(req)

	if err != nil {
		return nil, err
	}

	if len(res) < 1 {
		return nil, nil
	}

	return res[0][3], nil
}

func (conn *Connection) MemSet(key string, value []byte, expires uint32) error {
	// type memcacheMetaInfo struct {
	//     expires uint32
	//     flags   uint32
	//     cas     uint64
	// }

	cas := atomic.AddUint64(&conn.memcacheCas, 1)

	// WTF? See BenchmarkConcatBytes*
	b1 := PackInt(expires)
	b3 := PackLong(cas)
	metaInfo := []byte{
		b1[0], b1[1], b1[2], b1[3],
		0x0, 0x0, 0x0, 0x0,
		b3[0], b3[1], b3[2], b3[3],
		b3[4], b3[5], b3[6], b3[7],
	}

	data := Tuple{
		[]byte(key),
		metaInfo,
		[]byte(fmt.Sprintf(" 0 %d\r\n", len(value))), // @TODO: benchmark
		value,
	}

	_, err := conn.Execute(&Insert{
		Space: conn.memcacheSpace,
		Tuple: data,
	})

	return err
}

func (conn *Connection) MemDelete(key string) error {
	_, err := conn.Execute(&Delete{
		Space: conn.memcacheSpace,
		Value: Bytes(key),
	})

	return err
}
