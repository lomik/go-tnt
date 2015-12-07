package tnt

import (
	"encoding/base64"
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

func python(code string) []byte {
	cmd := exec.Command("python", "-c", code)
	out, err := cmd.Output()

	if err != nil {
		log.Fatal(err.Error())
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
		log.Fatal("error:", err)
		return []byte{}
	}

	return data
}

func randomInt(min int, max int) int {
	if min == max {
		return min
	}
	return rand.Intn(max-min) + min
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

	assert.Equal(
		pythonIproto("struct_B.pack(0)"),
		PackB(0),
	)

	for x := uint(0); x < 8; x++ {
		value := randomInt(1<<x, (1<<(x+1))-1)

		assert.Equal(
			pythonIproto("struct_B.pack(%d)", value),
			PackB(value),
		)
	}
}
