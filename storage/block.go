package storage

import (
    "errors"
    "fmt"
    "github.com/hadyn/goscape/types"
)

const (
    BlockHeaderLength = 8
    BlockLength       = 520
    BytesPerBlock     = BlockLength - BlockHeaderLength
    EndOfEntry        = 0
)

type Block struct {
    id          uint32
    volumeId    uint8
    entryId     uint16
    part        uint16
    nextBlockId uint32
    bytes       []byte
}

func (b *Block) Validate(storageId uint8, entryId uint16, part uint16) (error) {
    if storageId != b.volumeId || entryId != b.entryId || part != b.part {
        return errors.New(fmt.Sprintf(
            "block header mismatch: Storage: (expected: %d, actual: %d), Entry: (expected: %d, actual: %d), "+
                "Part: (expected: %d, actual: %d)", storageId, b.volumeId, entryId, b.entryId, part, b.part))
    }
    return nil
}

func (b *Block) Write(buffer []byte) {
    _ = buffer[BlockLength-1]
    types.BigEndian.PutUint16(buffer[0:], b.entryId)
    types.BigEndian.PutUint16(buffer[2:], b.part)
    types.BigEndian.PutUint24(buffer[4:], b.nextBlockId)
    buffer[7] = b.volumeId
    copy(buffer[8:], b.bytes)
}
