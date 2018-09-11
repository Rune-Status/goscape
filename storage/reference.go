package storage

import (
    "github.com/hadyn/goscape/types"
)

const (
    ReferenceLength = 6
)

type Reference struct {
    id      uint16
    length  uint32
    blockId uint32
}

func (r *Reference) Write(buffer []byte) {
    types.BigEndian.PutUint24(buffer[0:], r.length)
    types.BigEndian.PutUint24(buffer[3:], r.blockId)
}
