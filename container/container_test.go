package container

import (
    "testing"
    "github.com/hadyn/goscape/types"
    "bytes"
    "encoding/base64"
)

func TestUnpackContainerNoCompression(t *testing.T) {
    contents := []byte("Hello world!")

    buffer := make([]byte, ShortHeaderLength+len(contents))

    buffer[0] = byte(None)
    types.BigEndian.PutUint32(buffer[1:], uint32(len(contents)))

    copy(buffer[ShortHeaderLength:], contents)

    unpacked, err := Unpack(buffer)
    if err != nil {
        t.Fatalf("failed to unpack the container: %s", err)
    }

    if !bytes.Equal(unpacked, contents) {
        t.Error("bytes mismatch")
    }
}

func TestUnpackContainerBzip2(t *testing.T) {
    text := []byte("Hello world!")

    contents, err := base64.StdEncoding.DecodeString(
        "QlpoOTFBWSZTWQNY9XcAAAEVgGAAAEAGBJCAIAAxBkxBA0wi4Itio54u5IpwoSAGseru")
    if err != nil {
        t.Fatal("failed to decode base64 string")
    }

    buffer := make([]byte, LongHeaderLength+len(contents))

    buffer[0] = byte(Bzip2)
    types.BigEndian.PutUint32(buffer[1:], uint32(len(contents)))
    types.BigEndian.PutUint32(buffer[5:], uint32(len(text)))
    copy(buffer[LongHeaderLength:], contents[len(Bz2Header):]) // Trim off the header

    unpacked, err := Unpack(buffer)
    if err != nil {
        t.Fatalf("failed to unpack the container: %s", err)
    }

    if !bytes.Equal(unpacked, text) {
        t.Error("bytes mismatch")
    }
}

func TestUnpackContainerGzip(t *testing.T) {
    text := []byte("Hello world!")

    contents, err := base64.StdEncoding.DecodeString(
        "H4sIAAAAAAAA//NIzcnJVyjPL8pJUQQAlRmFGwwAAAA=")
    if err != nil {
        t.Fatal("failed to decode base64 string")
    }

    buffer := make([]byte, LongHeaderLength+len(contents))

    buffer[0] = byte(Gzip)
    types.BigEndian.PutUint32(buffer[1:], uint32(len(contents)))
    types.BigEndian.PutUint32(buffer[5:], uint32(len(text)))
    copy(buffer[LongHeaderLength:], contents)

    unpacked, err := Unpack(buffer)
    if err != nil {
        t.Fatalf("failed to unpack the container: %s", err)
    }

    if !bytes.Equal(unpacked, text) {
        t.Error("bytes mismatch")
    }
}

func TestContainerRoundTripNoCompression(t *testing.T) {
    contents := []byte("Hello world!")

    packed, err := Pack(contents, None)
    if err != nil {
        t.Fatalf("failed to pack the bytes: %s", err)
    }

    unpacked, err := Unpack(packed)

    if !bytes.Equal(unpacked, contents) {
        t.Error("bytes mismatch")
    }
}

func TestContainerRoundTripBzip2(t *testing.T) {
    contents := []byte("Hello world!")

    packed, err := Pack(contents, Bzip2)
    if err != nil {
        t.Fatalf("failed to pack the bytes: %s", err)
    }

    unpacked, err := Unpack(packed)

    if !bytes.Equal(unpacked, contents) {
        t.Error("bytes mismatch")
    }
}

func TestContainerRoundTripGzip(t *testing.T) {
    contents := []byte("Hello world!")

    packed, err := Pack(contents, Gzip)
    if err != nil {
        t.Fatalf("failed to pack the bytes: %s", err)
    }

    unpacked, err := Unpack(packed)

    if !bytes.Equal(unpacked, contents) {
        t.Error("bytes mismatch")
    }
}