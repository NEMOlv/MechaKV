package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GenerateKey get formatted key, for test only
func GenerateKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%09d", i))
}

// GenerateValue generate random value, for test only
func GenerateValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("value-" + string(b))
}
