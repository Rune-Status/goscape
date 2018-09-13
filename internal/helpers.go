package internal

func SequentialBytes(len int) []byte {
    buffer := make([]byte, len)
    for i := 0; i < len; i++ {
        buffer[i] = byte(i)
    }
    return buffer
}