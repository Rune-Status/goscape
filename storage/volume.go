package storage

import (
    "os"
    "sync"
    "github.com/hadyn/goscape/types"
    "errors"
    "fmt"
)

type Volume struct {
    id         uint8
    references *os.File
    blocks     *os.File
    mutex      *sync.Mutex
}

func NewVolume(id uint8, references *os.File, blocks *os.File, mutex *sync.Mutex) *Volume {
    return &Volume{
        id:         id,
        references: references,
        blocks:     blocks,
        mutex:      mutex,
    }
}

func (v Volume) Read(id uint16) ([]byte, error) {
    v.mutex.Lock()
    defer v.mutex.Unlock()

    // Read the reference.
    ref, err := v.readReference(id)
    if err != nil {
        return nil, err
    }

    length := uint32(ref.length)
    buffer := make([]byte, length)

    // Begin reading the entry.
    blockId := ref.blockId
    offset := uint32(0)
    for part := uint16(0); offset < length; part++ {
        if blockId == EndOfEntry {
            return nil, errors.New("premature end of entry")
        }

        block, err := v.readBlock(blockId)
        if err != nil {
            return nil, err
        }

        // Validate the block to make sure the entry is not corrupted or invalid.
        if err := block.Validate(v.id, id, part); err != nil {
            return nil, err
        }

        blockId = block.nextBlockId

        // Determine how many bytes to read this pass.
        read := length - offset
        if read > BytesPerBlock {
            read = BytesPerBlock
        }

        copy(buffer[offset:], block.bytes[:read])
        offset += read
    }

    return buffer, nil
}

func (v Volume) Write(id uint16, buffer []byte) error {
    v.mutex.Lock()
    defer v.mutex.Unlock()

    // First attempt to overwrite the entry if it exists and is valid,
    // otherwise append the entry.
    if err := v.write(id, buffer, true); err != nil {
        return v.write(id, buffer, false)
    }

    return nil
}

func (v Volume) write(id uint16, buffer []byte, overwrite bool) error {
    length := uint32(len(buffer))

    // Determine the next block identifier depending if we are overwriting the entry.
    var blockId uint32
    if overwrite {
        ref, err := v.readReference(id)
        if err != nil {
            return err
        }

        if exists, err := v.blockExists(ref.blockId); !exists || err != nil {
            if err != nil {
                return err
            }
            return errors.New(fmt.Sprintf("block %d does not exist", ref.blockId))
        }

        blockId = ref.blockId
    } else {
        nextBlockId, err := v.nextBlockId()
        if err != nil {
            return err
        }

        blockId = nextBlockId
    }

    // Update the reference.
    if err := v.writeReference(Reference{
        id:      id,
        length:  length,
        blockId: blockId,
    }); err != nil {
        return err
    }

    // Begin writing the entry.
    offset := uint32(0)
    for part := uint16(0); offset < length; part++ {
        nextBlockId := uint32(EndOfEntry)

        // If we are overwriting, determine the next block identifier and check that
        // the block/next block is valid.
        if overwrite {
            block, err := v.readBlock(blockId)
            if err != nil {
                return err
            }

            if err := block.Validate(v.id, id, part); err != nil {
                return err
            }

            if exists, err := v.blockExists(block.nextBlockId); !exists || err != nil {
                if err != nil {
                    return err
                }
                return errors.New(fmt.Sprintf("block %d does not exist", block.nextBlockId))
            }

            nextBlockId = block.nextBlockId
        }

        // Special case when we are not overwriting and or we reached
        // the end of the entry. Determine the next block identifier
        // from the end of the blocks file.
        if nextBlockId == EndOfEntry {
            freeBlockId, err := v.nextBlockId()
            if err != nil {
                return err
            }

            if blockId == freeBlockId {
                freeBlockId++
            }

            nextBlockId = freeBlockId
            overwrite = false
        }

        // Determine how many bytes we are writing this pass.
        write := length - offset
        if write <= BytesPerBlock {
            nextBlockId = EndOfEntry
        }

        if write > BytesPerBlock {
            write = BytesPerBlock
        }

        // Write the block.
        err := v.writeBlock(Block{
            id:          blockId,
            volumeId:    v.id,
            entryId:     id,
            part:        part,
            nextBlockId: nextBlockId,
            bytes:       buffer[offset : offset+write],
        })

        if err != nil {
            return err
        }

        blockId = nextBlockId
        offset += write
    }

    return nil
}

func (v Volume) readReference(id uint16) (Reference, error) {
    if _, err := v.references.Seek(int64(id*ReferenceLength), 0); err != nil {
        return Reference{}, err
    }

    buffer := make([]byte, ReferenceLength)
    if _, err := v.references.Read(buffer); err != nil {
        return Reference{}, err
    }

    return Reference{
        id:      id,
        length:  types.BigEndian.Uint24(buffer[0:]),
        blockId: types.BigEndian.Uint24(buffer[3:]),
    }, nil
}

func (v Volume) writeReference(ref Reference) error {
    if _, err := v.references.Seek(int64(ref.id*ReferenceLength), 0); err != nil {
        return err
    }

    buffer := make([]byte, ReferenceLength)
    ref.Write(buffer)

    if _, err := v.references.Write(buffer); err != nil {
        return err
    }

    return nil
}

func (v Volume) readBlock(id uint32) (Block, error) {
    if _, err := v.blocks.Seek(int64(id*BlockLength), 0); err != nil {
        return Block{}, err
    }

    buffer := make([]byte, BlockLength)
    if _, err := v.blocks.Read(buffer); err != nil {
        return Block{}, err
    }

    return Block{
        id:          id,
        volumeId:    buffer[7],
        entryId:     types.BigEndian.Uint16(buffer[0:]),
        part:        types.BigEndian.Uint16(buffer[2:]),
        nextBlockId: types.BigEndian.Uint24(buffer[4:]),
        bytes:       buffer[HeaderLength:],
    }, nil

}

func (v Volume) writeBlock(block Block) error {
    if _, err := v.blocks.Seek(int64(block.id*BlockLength), 0); err != nil {
        return err
    }

    buffer := make([]byte, BlockLength)
    block.Write(buffer)

    if _, err := v.blocks.Write(buffer); err != nil {
        return err
    }

    return nil
}

func (v Volume) blockExists(id uint32) (bool, error) {
    stat, err := v.blocks.Stat()
    if err != nil {
        return false, err
    }

    return id > EndOfEntry && id <= uint32(stat.Size()/BlockLength), nil
}

func (v Volume) nextBlockId() (uint32, error) {
    stat, err := v.blocks.Stat()
    if err != nil {
        return 0, err
    }

    id := uint32((stat.Size() + BlockLength - 1) / BlockLength)
    if id == EndOfEntry {
        id++
    }

    return id, nil
}
