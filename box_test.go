package tnt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBox(t *testing.T) {

	config := `
	space[0].enabled = 1
	space[0].index[0].type = "HASH"
	space[0].index[0].unique = 1
	space[0].index[0].key_field[0].fieldno = 0
	space[0].index[0].key_field[0].type = "NUM"
    `

	box, err := NewBox(config)
	require.NoError(t, err)
	box.Close()
}

func TestBoxSnapshot(t *testing.T) {
	require := require.New(t)

	config := `
	space[0].enabled = 1
	space[0].index[0].type = "HASH"
	space[0].index[0].unique = 1
	space[0].index[0].key_field[0].fieldno = 0
	space[0].index[0].key_field[0].type = "NUM"
    `

	box, err := NewBox(config)
	require.NoError(err)
	defer box.Close()

	// snapshot has been created already
	filename, err := box.Snapshot()
	require.NoError(err)
	require.Contains(filename, "snap/00000000000000000001.snap")

	// try saving new snapshot, but box has no new tuples inserted
	filename, err = box.SaveSnapshot()
	require.NoError(err)
	require.Contains(filename, "snap/00000000000000000001.snap")

	// insert new tuples
	conn, err := Connect(box.Listen(), nil)
	require.NoError(err)
	_, err = conn.Execute(&Insert{Space: 0, Tuple: Tuple{PackInt(1)}})
	require.NoError(err)

	filename, err = box.SaveSnapshot()
	require.NoError(err)
	require.Contains(filename, "snap/00000000000000000002.snap")
}
