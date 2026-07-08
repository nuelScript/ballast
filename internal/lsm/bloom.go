package lsm

import (
	"encoding/binary"
	"hash/fnv"
)

// bloom is a Bloom filter using double hashing (h1 + i*h2) for its k probes.
type bloom struct {
	k    uint32
	bits []byte
}

func newBloom(n, bitsPerKey int) *bloom {
	m := n * bitsPerKey
	if m < 64 {
		m = 64
	}
	k := uint32(float64(bitsPerKey) * 0.69) // ln2
	if k < 1 {
		k = 1
	}
	return &bloom{k: k, bits: make([]byte, (m+7)/8)}
}

func (b *bloom) add(key string) {
	h1, h2 := bloomHashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % m
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *bloom) mayContain(key string) bool {
	h1, h2 := bloomHashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % m
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloom) encode() []byte {
	out := make([]byte, 4+len(b.bits))
	binary.LittleEndian.PutUint32(out, b.k)
	copy(out[4:], b.bits)
	return out
}

func decodeBloom(data []byte) *bloom {
	return &bloom{k: binary.LittleEndian.Uint32(data), bits: data[4:]}
}

func bloomHashes(key string) (uint32, uint32) {
	h := fnv.New64a()
	h.Write([]byte(key))
	sum := h.Sum64()
	return uint32(sum), uint32(sum>>32) | 1 // h2 odd so probes don't collapse
}
