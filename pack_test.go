package tnt

import (
	"encoding/base64"
	"fmt"
	"log"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func python(code string) []byte {
	cmd := exec.Command("python", "-c", code)
	out, err := cmd.Output()

	if err != nil {
		log.Fatal(err.Error())
		return []byte{}
	}

	return []byte(out)
}

func pythonIproto(code string) []byte {
	res := python(
		fmt.Sprintf(
			"import python_iproto, sys; sys.stdout.write(str(python_iproto.%s).encode(\"base64\").strip())",
			code,
		),
	)

	data, err := base64.StdEncoding.DecodeString(string(res))
	if err != nil {
		log.Fatal("error:", err)
		return []byte{}
	}

	return data
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
