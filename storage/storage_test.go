package storage

import (
    "testing"
    "io/ioutil"
    "os"
    "bytes"
    "sync"
    "github.com/hadyn/goscape/types"
    "github.com/hadyn/goscape/internal"
)

func TestVolumeRead(t *testing.T) {
    dir, err := ioutil.TempDir("", "tmp")
    if err != nil {
        t.Fatal("failed to open the directory", err)
    }

    defer os.RemoveAll(dir)

    references, err := ioutil.TempFile(dir, "references")
    if err != nil {
        t.Fatal("failed to open the references file", err)
    }

    contents := internal.SequentialBytes(1000000)
    buffer := make([]byte, BlockLength)
    blockId := uint32(1)

    reference := &Reference{0,uint32(len(contents)), blockId}
    reference.Write(buffer)

    if _, err = references.Write(buffer[0:ReferenceLength]); err != nil {
        t.Fatal("failed to write the header", err)
    }

    blocks, err := ioutil.TempFile(dir, "blocks")
    if err != nil {
        t.Fatal("failed to open the blocks file", err)
    }

    part := uint16(0)
    for i := 0; i < len(contents); i += BytesPerBlock {
        length := len(contents) - i
        if length > BytesPerBlock {
            length = BytesPerBlock
        }

        block := &Block{blockId, 0, 0, part, blockId+1, contents[i:i+length] }
        block.Write(buffer)

        if _, err = blocks.Seek(int64(blockId*BlockLength), 0); err != nil {
            t.Fatal("failed to seek in blocks file", err)
        }

        if _, err = blocks.Write(buffer[0:BlockLength]); err != nil {
            t.Fatal("failed to write block", err)
        }

        blockId++
        part++
    }

    volume := NewVolume(0, references, blocks, &sync.Mutex{})

    entry, err := volume.Read(0)
    if err != nil {
        t.Fatal("failed to read the entry", err)
    }

    if !bytes.Equal(entry, contents) {
        t.Error("contents mismatch")
    }
}

func TestVolumeWriteAppend(t *testing.T) {
    dir, err := ioutil.TempDir("", "tmp")
    if err != nil {
        t.Fatal("failed to open the directory", err)
    }

    defer os.RemoveAll(dir)

    references, err := ioutil.TempFile(dir, "references")
    if err != nil {
        t.Fatal("failed to open the references file", err)
    }

    blocks, err := ioutil.TempFile(dir, "blocks")
    if err != nil {
        t.Fatal("failed to open the blocks file", err)
    }

    volumeId := uint8(0)
    entryId := uint16(0)
    contents := internal.SequentialBytes(1000000)
    length := uint32(len(contents))

    volume := NewVolume(volumeId, references, blocks, &sync.Mutex{})
    if err := volume.Write(entryId, contents); err != nil {
        t.Fatal("failed to write the entry")
    }

    buffer := make([]byte, BlockLength)

    if _, err := references.Seek(int64(entryId*ReferenceLength), 0); err != nil {
        t.Fatal("failed to seek to reference")
    }

    if _, err := references.Read(buffer[0:ReferenceLength]); err != nil {
        t.Fatal("failed to read the reference bytes")
    }

    if types.BigEndian.Uint24(buffer[0:]) != length {
        t.Errorf("length mismatch (expected: %d, actual: %d)", length, types.BigEndian.Uint24(buffer[0:]))
    }

    var blockId uint32
    if blockId = types.BigEndian.Uint24(buffer[3:]); blockId != 1 {
        t.Errorf("block identifier mismatch (expected: %d, actual: %d)", 1, blockId)
    }

    compare := make([]byte, length)

    offset := uint32(0)
    for part := uint16(0); offset < length; part++ {
        if blockId == EndOfEntry {
            t.Fatal("unexpected end of entry")
        }

        if _, err := blocks.Seek(int64(blockId*BlockLength), 0); err != nil {
            t.Fatal("failed to seek in blocks file")
        }

        if _, err := blocks.Read(buffer[:BlockLength]); err != nil {
            t.Fatal("failed to read the block bytes")
        }

        if types.BigEndian.Uint16(buffer[0:]) != entryId {
            t.Fatalf("entry identifier mismatch (expected: %d, actual: %d)",
                entryId, types.BigEndian.Uint16(buffer[0:]))
        }

        if types.BigEndian.Uint16(buffer[2:]) != part {
            t.Fatalf("part mismatch (expected: %d, actual: %d)",
                part, types.BigEndian.Uint16(buffer[2:]))
        }

        if buffer[7] != volumeId {
            t.Fatalf("volume identifier mismatch (expected: %d, actual: %d)", volumeId, buffer[7])
        }

        nextBlockId := types.BigEndian.Uint24(buffer[4:])

        read := length - offset
        if read > BytesPerBlock {
            read = BytesPerBlock
        }

        copy(compare[offset:], buffer[BlockHeaderLength:BlockHeaderLength+read])

        blockId = nextBlockId
        offset += read
    }

    if !bytes.Equal(compare, contents) {
        t.Error("bytes mismatch")
    }
}