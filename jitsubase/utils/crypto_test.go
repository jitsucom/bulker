package utils

import (
	"crypto/rsa"
	"github.com/stretchr/testify/require"
	"testing"
)

const pkWithPassphrase = ``

const pkWithoutPassphrase = ``

func TestParsePrivateKey(t *testing.T) {
	t.Skip()
	_, err := ParsePrivateKey([]byte(pkWithPassphrase), "test2")
	require.ErrorContains(t, err, "incorrect password")
	pk, err := ParsePrivateKey([]byte(pkWithPassphrase), "test")
	require.NoError(t, err)
	require.IsType(t, &rsa.PrivateKey{}, pk)

	_, err = ParsePrivateKey([]byte(pkWithoutPassphrase), "test2")
	require.ErrorContains(t, err, "only PKCS #5 v2.0 supported")
	pk, err = ParsePrivateKey([]byte(pkWithoutPassphrase), "")
	require.NoError(t, err)
	require.IsType(t, &rsa.PrivateKey{}, pk)
}
