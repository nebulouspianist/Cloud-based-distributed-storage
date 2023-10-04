package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string //key = hash of server names, value = server names
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	serverHashes := make([]string, 0)

	for h := range c.ServerMap {
		serverHashes = append(serverHashes, h)
	}

	sort.Strings(serverHashes)

	result := ""

	// block id is already the hash value of the block data

	for i := 0; i < len(serverHashes); i++ {

		if serverHashes[i] > blockId {
			result = c.ServerMap[serverHashes[i]]
			break
		}

	}

	if result == "" {
		result = c.ServerMap[serverHashes[0]]
	}

	return result

}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")

	serverMap := make(map[string]string)

	result := &ConsistentHashRing{}

	for _, addr := range serverAddrs {

		updatedAddr := "blockstore" + addr

		serverHash := result.Hash(updatedAddr)

		serverMap[serverHash] = addr

	}

	result.ServerMap = serverMap

	return result
}
