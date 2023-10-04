package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mutex              sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")

	fileName := fileMetaData.Filename
	ver := fileMetaData.Version

	m.mutex.Lock()

	_, ok := m.FileMetaMap[fileName]

	if ok {
		if ver == m.FileMetaMap[fileName].Version+1 {
			m.FileMetaMap[fileName] = fileMetaData
		} else {
			ver = -1
		}
	} else {
		m.FileMetaMap[fileName] = fileMetaData
	}

	m.mutex.Unlock()

	return &Version{Version: ver}, nil

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")

	// for each input hash, find their responsible server
	address_to_hash := make(map[string]*BlockHashes)

	m.mutex.Lock()
	for _, h := range blockHashesIn.Hashes {

		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(h)

		_, ok := address_to_hash[responsibleServer]

		if !ok {
			address_to_hash[responsibleServer] = &BlockHashes{}
		}

		address_to_hash[responsibleServer].Hashes = append(address_to_hash[responsibleServer].Hashes, h)

	}

	m.mutex.Unlock()

	return &BlockStoreMap{BlockStoreMap: address_to_hash}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")

	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
