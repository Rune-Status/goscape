package storage

import (
    "sync"
    "os"
    "path"
)

type Storage struct {
    root     string
    provider NameProvider
    blocks   *os.File
    mutex    *sync.Mutex
}

type NameProvider interface {
    index(id uint8) string
    blocks() string
}

func NewStorage(root string, provider NameProvider) (*Storage, error) {
    blocks, err := os.Open(path.Join(root, provider.blocks()))
    if err != nil {
        return nil, err
    }

    return &Storage{
        root:     root,
        provider: provider,
        blocks:   blocks,
        mutex:    &sync.Mutex{},
    }, nil
}

func (s *Storage) Open(id uint8) (*Volume, error) {
    references, err := os.Open(path.Join(s.root, s.provider.index(id)))
    if err != nil {
        return nil, err
    }
    return NewVolume(id, references, s.blocks, s.mutex), nil
}
