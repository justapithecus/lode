package lode

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
)

// -----------------------------------------------------------------------------
// MD5 Checksum
// -----------------------------------------------------------------------------

// md5Checksum implements Checksum using MD5.
type md5Checksum struct{}

// NewMD5Checksum creates an MD5 checksum component.
//
// MD5 produces 128-bit hashes represented as 32 hex characters.
// Use with WithChecksum to enable checksums for a dataset.
func NewMD5Checksum() Checksum {
	return &md5Checksum{}
}

func (c *md5Checksum) Name() string {
	return "md5"
}

func (c *md5Checksum) NewHasher() HashWriter {
	return &hashWriter{h: md5.New()}
}

// hashWriter wraps a hash.Hash to implement HashWriter.
type hashWriter struct {
	h hash.Hash
}

func (hw *hashWriter) Write(p []byte) (n int, err error) {
	return hw.h.Write(p)
}

func (hw *hashWriter) Sum() string {
	return hex.EncodeToString(hw.h.Sum(nil))
}
