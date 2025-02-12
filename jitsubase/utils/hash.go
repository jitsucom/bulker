package utils

import (
	"crypto/md5"
	"fmt"
	"github.com/mitchellh/hashstructure/v2"
	"hash/fnv"
)

var hashOptions = &hashstructure.HashOptions{SlicesAsSets: true}

func HashString(value string) [16]byte {
	return md5.Sum([]byte(value))
}

func HashStringS(value string) string {
	sum := md5.Sum([]byte(value))
	return fmt.Sprintf("%x", sum)
}

func HashStringInt(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
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
