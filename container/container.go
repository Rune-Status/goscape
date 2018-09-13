package container

import (
    "io"
    "compress/gzip"
    "github.com/dsnet/compress/bzip2"
    "bytes"
    "github.com/hadyn/goscape/types"
    "errors"
)

type Compression uint8

var (
    Bz2Header                   = []byte("BZh9")
    UnsupportedCompressionError = errors.New("unsupported compression")
)

const (
    ShortHeaderLength             = 5
    LongHeaderLength              = 9
    None              Compression = 0
    Bzip2             Compression = 1
    Gzip              Compression = 2
)

func Unpack(buffer []byte) ([]byte, error) {
    compression := Compression(buffer[0])
    payloadLength := types.BigEndian.Uint32(buffer[1:])

    var length int
    switch compression {
    case None:
        length = int(payloadLength)
    case Bzip2, Gzip:
        length = int(types.BigEndian.Uint32(buffer[ShortHeaderLength:]))
    default:
        return nil, UnsupportedCompressionError
    }

    headerLength, err := compression.headerLength()
    if err != nil {
        return nil, err
    }

    reader, err := compression.reader(buffer[:uint32(headerLength)+payloadLength])
    if err != nil {
        return nil, err
    }

    if closer, ok := reader.(io.Closer); ok {
        defer closer.Close()
    }

    result := make([]byte, length)

    for offset := 0; offset < length; {
        read, err := reader.Read(result[offset : length-offset])
        if err != nil {
            switch err {
            case io.EOF:
                if offset+read < length {
                    return nil, err
                }
            default:
                return nil, err
            }
        }
        offset += read
    }

    return result, nil
}

func Pack(buffer []byte, compression Compression) ([]byte, error) {
    var buf bytes.Buffer

    writer, err := compression.writer(&buf)
    if err != nil {
        return nil, err
    }

    headerLength, err := compression.headerLength()
    if err != nil {
        return nil, err
    }

    _, err = writer.Write(buffer)
    if err != nil {
        return nil, err
    }

    if closer, ok := writer.(io.Closer); ok {
        closer.Close()
    }

    result := make([]byte, headerLength + buf.Len())

    result[0] = byte(compression)
    types.BigEndian.PutUint32(result[1:], uint32(buf.Len()))

    switch compression {
    case Bzip2, Gzip:
        types.BigEndian.PutUint32(result[5:], uint32(len(buffer)))
    }

    compressed := buf.Bytes()

    switch compression {
    case Bzip2:
        // Strip the BZ2 header.
        copy(result[headerLength:], compressed[len(Bz2Header):])
    default:
        copy(result[headerLength:], compressed)
    }

    return result, nil
}

func (c Compression) reader(buffer []byte) (io.Reader, error) {
    switch c {
    case None:
        return bytes.NewReader(buffer[ShortHeaderLength:]), nil
    case Bzip2:
        return bzip2.NewReader(
            bytes.NewReader(append(Bz2Header,
                buffer[LongHeaderLength:]...)), &bzip2.ReaderConfig{})
    case Gzip:
        return gzip.NewReader(bytes.NewReader(buffer[LongHeaderLength:]))
    default:
        return nil, UnsupportedCompressionError
    }
}

func (c Compression) writer(writer io.Writer) (io.Writer, error) {
    switch c {
    case None:
        return writer, nil
    case Bzip2:
        return bzip2.NewWriter(writer, &bzip2.WriterConfig{Level:9})
    case Gzip:
        return gzip.NewWriter(writer), nil
    default:
        return nil, UnsupportedCompressionError
    }
}

func (c Compression) headerLength() (int, error) {
    switch c {
    case None:
        return ShortHeaderLength, nil
    case Bzip2, Gzip:
        return LongHeaderLength, nil
    default:
        return 0, UnsupportedCompressionError
    }
}