package utils

import (
	"encoding/pem"
	"fmt"
	"github.com/youmark/pkcs8"
)

func ParsePrivateKey(privateKey []byte, passphrase string) (pk interface{}, err error) {
	block, _ := pem.Decode(privateKey)
	if block == nil {
		return nil, fmt.Errorf("Failed to decode PEM block")
	}
	pk, _, err = pkcs8.ParsePrivateKey(block.Bytes, []byte(passphrase))
	return pk, err
}
