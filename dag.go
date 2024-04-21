package merkledag

import (
    "encoding/json"
    "errors"
    "hash"
    "strconv"
)

const (
    K         = 1 << 10
    FILE_SIZE = 256 * K
)

type Link struct {
    Name string
    Hash []byte
    Size int
}

type Object struct {
    Links []Link
    Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) ([]byte, error) {
    switch node.Type() {
    case FILE:
        return handleFileNode(store, node.(File), h)
    case DIR:
        return handleDirNode(store, node.(Dir), h)
    default:
        return nil, errors.New("unsupported node type")
    }
}

func handleFileNode(store KVStore, fileNode File, h hash.Hash) ([]byte, error) {
    fileData := fileNode.Bytes()
    fileSize := len(fileData)
    if fileSize > FILE_SIZE {
        return handleLargeFile(store, fileData, fileSize, h)
    }
    return storeObject(store, nil, fileData, h)
}

func handleLargeFile(store KVStore, data []byte, size int, h hash.Hash) ([]byte, error) {
    list := Object{Data: []byte("list")}
    for i := 0; i*size/FILE_SIZE < len(data); i++ {
        start := i * FILE_SIZE
        end := start + FILE_SIZE
        if end > len(data) {
            end = len(data)
        }
        partData := data[start:end]
        hash, err := storeObject(store, nil, partData, h)
        if err != nil {
            return nil, err
        }
        list.Links = append(list.Links, Link{Name: "part" + strconv.Itoa(i), Hash: hash, Size: end - start})
    }
    return storeObject(store, list.Links, list.Data, h)
}

func handleDirNode(store KVStore, dirNode Dir, h hash.Hash) ([]byte, error) {
    tree := Object{Data: []byte("tree")}
    it := dirNode.It()
    for it.Next() {
        child := it.Node()
        childHash, err := Add(store, child, h)
        if err != nil {
            return nil, err
        }
        tree.Links = append(tree.Links, Link{Name: child.Name(), Hash: childHash, Size: len(child.Bytes())})
    }
    return storeObject(store, tree.Links, tree.Data, h)
}

func storeObject(store KVStore, links []Link, data []byte, h hash.Hash) ([]byte, error) {
    obj := Object{Links: links, Data: data}
    objBytes, err := json.Marshal(obj)
    if err != nil {
        return nil, err
    }
    h.Reset()
    h.Write(objBytes)
    hash := h.Sum(nil)
    store.Put(hash, objBytes)
    return hash, nil
}
