package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)

	if err != nil {
		conn.Close()
		return err
	}

	*succ = success.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = b.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// panic("todo")

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	returnedHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = returnedHashes.Hashes

	return conn.Close()

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")

	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		file, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {

			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}

			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}

			conn.Close()
			return err
		}

		*serverFileInfoMap = file.FileInfoMap

		return conn.Close()
	}

	return fmt.Errorf("cannot find leader or all servers are down.")

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")

	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ver, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {

			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}

			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}

			conn.Close()
			return err
		}

		*latestVersion = ver.Version

		return conn.Close()
	}

	return fmt.Errorf("cannot find leader or all servers are down.")

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// panic("todo")

	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		m, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

		if err != nil {

			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}

			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}

			conn.Close()
			return err
		}

		result := make(map[string][]string)

		for key, value := range m.BlockStoreMap {

			result[key] = value.Hashes
		}

		// fmt.Println("blockHashesin in rpcClient.go")
		// fmt.Println(blockHashesIn)

		// fmt.Println("output from rpcClient.go: ")
		// fmt.Println(result)

		*blockStoreMap = result

		return conn.Close()
	}

	return fmt.Errorf("cannot find leader or all servers are down.")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("todo")

	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		if err != nil {

			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}

			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}

			conn.Close()
			return err
		}

		*blockStoreAddrs = addrs.BlockStoreAddrs

		return conn.Close()
	}
	return fmt.Errorf("cannot find leader or all servers are down.")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
