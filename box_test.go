package tntctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBox(t *testing.T) {
	assert := assert.New(t)

	config := `
	space[0].enabled = 1
	space[0].index[0].type = "HASH"
	space[0].index[0].unique = 1
	space[0].index[0].key_field[0].fieldno = 0
	space[0].index[0].key_field[0].type = "NUM"
    `

	box, err := New(config, &Options{})

	if assert.NoError(err) {
		box.Close()
	}

}
