package types

type ByteOrder interface {
    Uint16([]byte) uint16
    Uint24([]byte) uint32
    Uint32([]byte) uint32
    Uint64([]byte) uint64
}

var BigEndian bigEndian

type bigEndian struct{}

func (bigEndian) Uint16(b []byte) uint16 {
    _ = b[1] // early bounds check to guarantee safety of writes below
    return uint16(b[1]) | uint16(b[0])<<8
}

func (bigEndian) Uint32(b []byte) uint32 {
    _ = b[3] // early bounds check to guarantee safety of writes below
    return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func (bigEndian) Uint24(b []byte) uint32 {
    _ = b[2] // early bounds check to guarantee safety of writes below
    return uint32(b[2]) | uint32(b[1])<<8 | uint32(b[0])<<16
}

func (bigEndian) Uint64(b []byte) uint64 {
    _ = b[7] // early bounds check to guarantee safety of writes below
    return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
        uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

func (bigEndian) PutUint16(b []byte, v uint16) {
    _ = b[1] // early bounds check to guarantee safety of writes below
    b[0] = byte(v >> 8)
    b[1] = byte(v)
}

func (bigEndian) PutUint24(b []byte, v uint32) {
    _ = b[2] // early bounds check to guarantee safety of writes below
    b[0] = byte(v >> 16)
    b[1] = byte(v >> 8)
    b[2] = byte(v)
}

func (bigEndian) PutUint32(b []byte, v uint32) {
    _ = b[3] // early bounds check to guarantee safety of writes below
    b[0] = byte(v >> 24)
    b[1] = byte(v >> 16)
    b[2] = byte(v >> 8)
    b[3] = byte(v)
}

func (bigEndian) PutUint64(b []byte, v uint64) {
    _ = b[7] // early bounds check to guarantee safety of writes below
    b[0] = byte(v >> 56)
    b[1] = byte(v >> 48)
    b[2] = byte(v >> 40)
    b[3] = byte(v >> 32)
    b[4] = byte(v >> 24)
    b[5] = byte(v >> 16)
    b[6] = byte(v >> 8)
    b[7] = byte(v)
}
