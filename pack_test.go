package tnt

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// iterator [0, N)
func N(n int) []struct{} {
	return make([]struct{}, n)
}

func randomInt(min int, max int) int {
	if min == max {
		return min
	}
	return rand.Intn(max-min) + min
}

// generate test values:
// * 0
// * 1
// * 2-3
// * 4-15
// ...
// * random from range [2**(k-1), 2**k-1]
// ...
// * 2**n-1
func values(n int) chan uint {
	ch := make(chan uint, n+2)

	ch <- 0
	for x := range N(n) {
		if x == 63 {
			ch <- uint(2 * randomInt(1<<uint(x-1), (1<<(uint(x-1)+1))))
		} else {
			ch <- uint(randomInt(1<<uint(x), (1 << (uint(x) + 1))))
		}
		// pp.Println(x)
	}
	ch <- 1<<uint(n) - 1
	close(ch)
	return ch
}

func python(code string) []byte {
	// pp.Println(code)
	cmd := exec.Command("python", "-c", code)
	out, err := cmd.Output()

	if err != nil {
		log.Fatal("python execute error:", err.Error())
		return []byte{}
	}

	return []byte(out)
}

func pythonIproto(code string, args ...interface{}) []byte {
	codeWithParams := fmt.Sprintf(code, args...)
	res := python(
		fmt.Sprintf(
			"import python_iproto, sys; sys.stdout.write(str(python_iproto.%s).encode(\"base64\").strip())",
			codeWithParams,
		),
	)

	data, err := base64.StdEncoding.DecodeString(string(res))
	if err != nil {
		log.Fatal("base64 decode error:", err)
		return []byte{}
	}

	return data
}

func TestValues(t *testing.T) {
	assert := assert.New(t)

	index := 0
	for value := range values(8) {
		switch index {
		case 0:
			assert.Equal(uint(0), value)
		case 1:
			assert.Equal(uint(1), value)
		case 9:
			assert.Equal(uint(255), value)
		default:
			assert.True(value >= (1 << uint(index-1)))
			assert.True(value < (1 << uint(index)))
		}

		index++
	}

	assert.Equal(10, index)
}

func TestPython(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(
		[]byte("BAEAAAA="),
		python("import python_iproto, sys; sys.stdout.write(str(python_iproto.pack_int(1)).encode(\"base64\").strip())"),
	)
}

func TestPythonIproto(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(
		[]byte{0x04, 0x01, 0x00, 0x00, 0x00},
		pythonIproto("pack_int(1)"),
	)
}

func TestPackB(t *testing.T) {
	assert := assert.New(t)

	for value := range values(8) {
		assert.Equal(
			pythonIproto("struct_B.pack(%d)", value),
			PackB(uint8(value)),
		)
	}
}

func TestPackInt(t *testing.T) {
	assert := assert.New(t)

	for value := range values(32) {
		assert.Equal(
			pythonIproto("struct_L.pack(%d)", value),
			PackInt(uint32(value)),
		)
	}
}

func TestPackLong(t *testing.T) {
	assert := assert.New(t)

	for value := range values(64) {
		assert.Equal(
			pythonIproto("struct_Q.pack(%d)", value),
			PackLong(uint64(value)),
		)
	}
}

func TestPackIntBase128(t *testing.T) {
	assert := assert.New(t)

	for value := range values(32) {
		assert.Equal(
			pythonIproto("pack_int_base128(%d)", value),
			PackIntBase128(uint32(value)),
		)
	}
}

func TestPackFieldStr(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(
		pythonIproto("pack_str(\"%s\")", "hello_world"),
		packFieldStr([]byte("hello_world")),
	)

	for value := range values(64) {
		assert.Equal(
			pythonIproto("pack_str(\"%d\")", value),
			packFieldStr(Bytes(fmt.Sprintf("%d", value))),
		)
	}
}

func TestPackTuple(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(
		pythonIproto("pack_tuple([10,42,15,\"hello world\"])"),
		packTuple(Tuple{
			PackInt(10),
			PackInt(42),
			PackInt(15),
			Bytes("hello world"),
		}),
	)
}

func TestPackSelect(t *testing.T) {
	assert := assert.New(t)

	v, _ := (&Select{Value: PackInt(42)}).Pack(0, 0)
	assert.Equal(pythonIproto("pack_select(0, 42)"), v)

	v, _ = (&Select{
		Values: Tuple{PackInt(11), PackInt(12)},
		Space:  10,
		Offset: 13,
		Limit:  14,
		Index:  15,
	}).Pack(0, 0)

	assert.Equal(
		pythonIproto("pack_select(10, [11, 12], offset=13, limit=14, index=15)"),
		v,
	)

	v, _ = (&Select{
		Tuples: []Tuple{
			Tuple{PackInt(11), PackInt(12)},
			Tuple{PackInt(13), PackInt(14)},
		},
		Space: 1,
	}).Pack(0, 0)

	assert.Equal(
		pythonIproto("pack_select(1, [[11, 12], [13, 14]])"),
		v,
	)
}

func TestPackInsert(t *testing.T) {
	assert := assert.New(t)

	v, _ := (&Insert{
		Tuple: Tuple{
			PackInt(42),
			PackInt(15),
		},
	}).Pack(0, 0)

	assert.Equal(
		pythonIproto("pack_insert(0, [42, 15])"),
		v,
	)

	v, _ = (&Insert{
		Space: 10,
		Tuple: Tuple{
			PackInt(11),
			PackInt(12),
		},
		ReturnTuple: true,
	}).Pack(0, 0)

	assert.Equal(
		pythonIproto("pack_insert(10, [11, 12], return_tuple=1)"),
		v,
	)
}

func BenchmarkPackSelect(b *testing.B) {
	for i := 0; i < b.N; i += 1 {
		request := &Select{
			Values: Tuple{
				Bytes("hello"),
				PackInt(42),
				PackInt(15),
			},
			Space:  0,
			Offset: 42,
			Limit:  100,
			Index:  0,
		}
		request.Pack(0, 0)
	}
}

// pack1 is the original Select.Pack.
func pack1(q *Select, requestID uint32, defaultSpace uint32) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var buf bytes.Buffer

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		bodyBuffer.Write(PackInt(uint32(i)))
	} else {
		bodyBuffer.Write(PackInt(defaultSpace))
	}
	bodyBuffer.Write(PackInt(q.Index))
	bodyBuffer.Write(PackInt(q.Offset))
	bodyBuffer.Write(PackInt(limit))

	if q.Value != nil {
		bodyBuffer.Write(PackInt(1))
		bodyBuffer.Write(packTuple(Tuple{q.Value}))
	} else if q.Values != nil {
		cnt := len(q.Values)
		bodyBuffer.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(Tuple{q.Values[i]}))
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		bodyBuffer.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(q.Tuples[i]))
		}
	} else {
		bodyBuffer.Write(packedInt0)
	}

	buf.Write(PackInt(requestTypeSelect))
	buf.Write(PackInt(uint32(bodyBuffer.Len())))
	buf.Write(PackInt(requestID))
	buf.Write(bodyBuffer.Bytes())

	return buf.Bytes(), nil
}

// pack2 is differ from pack1 by removing one bytes.Buffer.
func pack2(q *Select, requestID uint32, defaultSpace uint32) ([]byte, error) {
	//var bodyBuffer bytes.Buffer
	var buf bytes.Buffer

	buf.Write(PackInt(requestTypeSelect))
	buf.Write(PackInt(0xffffffff))
	buf.Write(PackInt(requestID))

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		buf.Write(PackInt(uint32(i)))
	} else {
		buf.Write(PackInt(defaultSpace))
	}

	buf.Write(PackInt(q.Index))
	buf.Write(PackInt(q.Offset))
	buf.Write(PackInt(limit))
	length := 16

	if q.Value != nil {
		buf.Write(PackInt(1)) // count
		//buffer.Write(packTuple(Tuple{q.Value}))
		buf.Write(PackInt(1)) // fields
		vlp := PackIntBase128(uint32(len(q.Value)))
		buf.Write(vlp)
		buf.Write(q.Value)
		length += 4 + 4 + len(vlp) + len(q.Value)
	} else if q.Values != nil {
		cnt := len(q.Values)
		buf.Write(PackInt(uint32(cnt)))
		length += 4
		for i := 0; i < cnt; i++ {
			buf.Write(PackInt(1)) // fields
			vlp := PackIntBase128(uint32(len(q.Values[i])))
			buf.Write(vlp)
			buf.Write(q.Values[i])
			length += 4 + len(vlp) + len(q.Values[i])
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		buf.Write(PackInt(uint32(cnt)))
		length += 4
		for i := 0; i < cnt; i++ {
			//buf.Write(packTuple(q.Tuples[i]))
			tuple := q.Tuples[i]
			fields := len(tuple)
			buf.Write(PackInt(uint32(fields)))
			length += 4
			for i := 0; i < fields; i++ {
				//buf.Write(packFieldStr(value[i]))
				vlp := PackIntBase128(uint32(len(tuple[i])))
				buf.Write(vlp)
				buf.Write(tuple[i])
				length += len(vlp) + len(tuple[i])
			}
		}
	} else {
		buf.Write(packedInt0)
		length += 4
	}

	tmpbuf := buf.Bytes()
	binary.LittleEndian.PutUint32(tmpbuf[4:8], uint32(length))

	return buf.Bytes(), nil
}

// pack3 uses pre-calculated length of the Query to Grow buffer in the beginning.
func pack3(q *Select, requestID uint32, defaultSpace uint32) ([]byte, error) {
	length := 20
	switch {
	case q.Value != nil:
		length += 4 + base128len(len(q.Value))
	case q.Values != nil:
		cnt := len(q.Values)
		for i := 0; i < cnt; i++ {
			length += 4 + base128len(len(q.Values[i]))
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		for i := 0; i < cnt; i++ {
			fields := len(q.Tuples[i])
			length += 4
			for j := 0; j < fields; j++ {
				length += base128len(len(q.Tuples[i][j]))
			}
		}
	}

	buf := new(bytes.Buffer)
	buf.Grow(length + 12)

	buf.Write(PackInt(requestTypeSelect))
	buf.Write(PackInt(uint32(length)))
	buf.Write(PackInt(requestID))

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		buf.Write(PackInt(uint32(i)))
	} else {
		buf.Write(PackInt(defaultSpace))
	}

	buf.Write(PackInt(q.Index))
	buf.Write(PackInt(q.Offset))
	buf.Write(PackInt(limit))

	if q.Value != nil {
		buf.Write(PackInt(1)) // count
		//buffer.Write(packTuple(Tuple{q.Value}))
		buf.Write(PackInt(1)) // fields
		vlp := PackIntBase128(uint32(len(q.Value)))
		buf.Write(vlp)
		buf.Write(q.Value)
	} else if q.Values != nil {
		cnt := len(q.Values)
		buf.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			buf.Write(PackInt(1)) // fields
			vlp := PackIntBase128(uint32(len(q.Values[i])))
			buf.Write(vlp)
			buf.Write(q.Values[i])
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		buf.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			//buf.Write(packTuple(q.Tuples[i]))
			tuple := q.Tuples[i]
			fields := len(tuple)
			buf.Write(PackInt(uint32(fields)))
			for i := 0; i < fields; i++ {
				//buf.Write(packFieldStr(value[i]))
				vlp := PackIntBase128(uint32(len(tuple[i])))
				buf.Write(vlp)
				buf.Write(tuple[i])
			}
		}
	} else {
		buf.Write(packedInt0)
	}

	return buf.Bytes(), nil
}

// pack4 uses simple byte slice and position marker with pre-calculated length.
// Remove PackInt function using to avoid redundant byte slices allocations.
func pack4(q *Select, requestID uint32, defaultSpace uint32) ([]byte, error) {
	length := 20
	switch {
	case q.Value != nil:
		length += 4 + base128len(len(q.Value))
	case q.Values != nil:
		cnt := len(q.Values)
		for i := 0; i < cnt; i++ {
			length += 4 + base128len(len(q.Values[i]))
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		for i := 0; i < cnt; i++ {
			fields := len(q.Tuples[i])
			length += 4
			for j := 0; j < fields; j++ {
				length += base128len(len(q.Tuples[i][j]))
			}
		}
	}

	buf := make([]byte, length+12)
	pos := 0

	binary.LittleEndian.PutUint32(buf[pos:pos+4], requestTypeSelect)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(length))
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], requestID)
	pos += 4

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(buf[pos:pos+4], i)
		pos += 4
	} else {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], defaultSpace)
		pos += 4
	}

	binary.LittleEndian.PutUint32(buf[pos:pos+4], q.Index)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], q.Offset)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], limit)
	pos += 4

	if q.Value != nil {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], 1) // count
		pos += 4
		binary.LittleEndian.PutUint32(buf[pos:pos+4], 1) // fields
		pos += 4
		vlp := PackIntBase128(uint32(len(q.Value)))
		copy(buf[pos:pos+len(vlp)], vlp)
		pos += len(vlp)
		copy(buf[pos:pos+len(q.Value)], q.Value)
		pos += len(q.Value)
	} else if q.Values != nil {
		cnt := len(q.Values)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(cnt))
		pos += 4
		for i := 0; i < cnt; i++ {
			binary.LittleEndian.PutUint32(buf[pos:pos+4], 1) // fields
			pos += 4
			vlp := PackIntBase128(uint32(len(q.Values[i])))
			copy(buf[pos:pos+len(vlp)], vlp)
			pos += len(vlp)
			copy(buf[pos:pos+len(q.Values[i])], q.Values[i])
			pos += len(q.Values[i])
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(cnt))
		pos += 4
		for i := 0; i < cnt; i++ {
			tuple := q.Tuples[i]
			fields := len(tuple)
			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(fields))
			pos += 4
			for j := 0; j < fields; j++ {
				vlp := PackIntBase128(uint32(len(tuple[j])))
				copy(buf[pos:pos+len(vlp)], vlp)
				pos += len(vlp)
				copy(buf[pos:pos+len(tuple[j])], tuple[j])
				pos += len(tuple[j])
			}
		}
	} else {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], 0) // count
		// increasing pos is redundant due to near return
	}

	return buf, nil
}

// pack5 is differ from pack4 by using FixedBuffer and Select.ByteLength for better readability.
func pack5(q *Select, requestID uint32, defaultSpace uint32) ([]byte, error) {
	length := q.ByteLength()

	buf := NewFixedBuffer(length + 12)

	buf.WriteUint32(requestTypeSelect)
	buf.WriteUint32(uint32(length))
	buf.WriteUint32(requestID)

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		buf.WriteUint32(i)
	} else {
		buf.WriteUint32(defaultSpace)
	}

	buf.WriteUint32(q.Index)
	buf.WriteUint32(q.Offset)
	buf.WriteUint32(limit)

	switch {
	case q.Value != nil:
		buf.WriteUint32(1) // count
		buf.WriteUint32(1) // fields
		vlp := PackIntBase128(uint32(len(q.Value)))
		buf.Write(vlp)
		buf.Write(q.Value)
	case q.Values != nil:
		cnt := len(q.Values)
		buf.WriteUint32(uint32(cnt))
		for i := 0; i < cnt; i++ {
			buf.WriteUint32(1) // fields
			vlp := PackIntBase128(uint32(len(q.Values[i])))
			buf.Write(vlp)
			buf.Write(q.Values[i])
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		buf.WriteUint32(uint32(cnt))
		for i := 0; i < cnt; i++ {
			tuple := q.Tuples[i]
			fields := len(tuple)
			buf.WriteUint32(uint32(fields))
			for j := 0; j < fields; j++ {
				vlp := PackIntBase128(uint32(len(tuple[j])))
				buf.Write(vlp)
				buf.Write(tuple[j])
			}
		}
	default:
		buf.WriteUint32(0) // count
	}

	return buf.Bytes(), nil
}

func TestPacksSelect(t *testing.T) {
	tt := []struct {
		req *Select
	}{
		{&Select{Space: 0, Offset: 42, Limit: 100, Index: 0}},
		{&Select{Value: Bytes("hello"), Space: 0, Offset: 42, Limit: 100, Index: 0}},
		{&Select{
			Values: Tuple{Bytes("hello"), PackInt(42), PackInt(15)},
			Space:  0,
			Offset: 42,
			Limit:  100,
			Index:  0,
		}},
		{&Select{
			Tuples: []Tuple{
				{Bytes("hello"), PackInt(42), PackInt(15)},
				{Bytes("hello2"), PackInt(420), PackInt(150)},
			},
			Space:  0,
			Offset: 42,
			Limit:  100,
			Index:  0,
		}},
	}
	for tc, item := range tt {
		expected, _ := pack1(item.req, 0, 0)
		actual, _ := pack2(item.req, 0, 0)
		assert.Equal(t, expected, actual, "case %v (pack2)", tc+1)
		actual, _ = pack3(item.req, 0, 0)
		assert.Equal(t, expected, actual, "case %v (pack3)", tc+1)
		actual, _ = pack4(item.req, 0, 0)
		assert.Equal(t, expected, actual, "case %v (pack4)", tc+1)
		actual, _ = pack5(item.req, 0, 0)
		assert.Equal(t, expected, actual, "case %v (pack5)", tc+1)
	}
}

func BenchmarkPacksSelect(b *testing.B) {
	bt := []struct {
		req *Select
	}{
		{&Select{Space: 0, Offset: 42, Limit: 100, Index: 0}},
		{&Select{Value: Bytes("hello"), Space: 0, Offset: 42, Limit: 100, Index: 0}},
		{&Select{
			Values: Tuple{Bytes("hello"), PackInt(42), PackInt(15)},
			Space:  0,
			Offset: 42,
			Limit:  100,
			Index:  0,
		}},
		{&Select{
			Tuples: []Tuple{
				{Bytes("hello"), PackInt(42), PackInt(15)},
				{Bytes("hello2"), PackInt(420), PackInt(150)},
			},
			Space:  0,
			Offset: 42,
			Limit:  100,
			Index:  0,
		}},
	}
	names := []string{"Empty", "Value", "Values", "Tuples"}
	for tc, item := range bt {
		b.Run("Pack1"+names[tc], SubBenchmarkPackSelect(pack1, item.req))
		b.Run("Pack2"+names[tc], SubBenchmarkPackSelect(pack2, item.req))
		b.Run("Pack3"+names[tc], SubBenchmarkPackSelect(pack3, item.req))
		b.Run("Pack4"+names[tc], SubBenchmarkPackSelect(pack4, item.req))
		b.Run("Pack5"+names[tc], SubBenchmarkPackSelect(pack5, item.req))
	}
}

func SubBenchmarkPackSelect(pack func(*Select, uint32, uint32) ([]byte, error), req *Select) func(*testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bs, _ := pack(req, 0, 0)
			_ = bs
		}
	}
}
