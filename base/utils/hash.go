package utils

import (
	"crypto/md5"
	"github.com/mitchellh/hashstructure/v2"
)

var hashOptions = &hashstructure.HashOptions{SlicesAsSets: true}

func HashString(value string) [16]byte {
	return md5.Sum([]byte(value))
}

func HashBytes(payload []byte) [16]byte {
	return md5.Sum(payload)
}

func HashAny(value interface{}) (uint64, error) {
	hash, err := hashstructure.Hash(value, hashstructure.FormatV2, hashOptions)
	if err != nil {
		return 0, err
	}

	return hash, nil
}
