package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mutex    sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")

	// panic("todo")

	bs.mutex.Lock()

	block := bs.BlockMap[blockHash.Hash]

	bs.mutex.Unlock()

	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")

	bs.mutex.Lock()

	hash := GetBlockHashString(block.BlockData)

	bs.BlockMap[hash] = block

	bs.mutex.Unlock()

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")

	hashes := make([]string, 0)

	for _, h := range blockHashesIn.Hashes {

		bs.mutex.Lock()

		_, ok := bs.BlockMap[h]

		if ok {
			hashes = append(hashes, h)
		}

		bs.mutex.Unlock()
	}

	return &BlockHashes{Hashes: hashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")

	hashes := make([]string, 0)

	bs.mutex.Lock()

	for key := range bs.BlockMap {
		hashes = append(hashes, key)
	}

	bs.mutex.Unlock()
	return &BlockHashes{Hashes: hashes}, nil

}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
