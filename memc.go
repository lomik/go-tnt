package tnt

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

func (conn *Connection) MemSet(key string, value []byte, expires uint32) {
	// cas := atomic.AddUint64(&conn.memcacheCas, 1)

	// metaInfo := make([]byte, 16)
	// metaInfo[:4] = PackInt(expires)
	// metaInfo[4:8] = PackInt(0)
	// metaInfo[8:16] = PackLong(cas)

	// type memcacheMetaInfo struct {
	//     expires uint32
	//     flags   uint32
	//     cas     uint64
	// }

	// type tupleMemcache struct {
	//     Key               string
	//     MetaInfo          string
	//     ReadableSomething string
	//     Value             []byte
	// }

}
