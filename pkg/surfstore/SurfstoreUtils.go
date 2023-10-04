package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")

	// first of all, create the index.db file
	indexPath := client.BaseDir + "/index.db"

	_, err := os.Stat(indexPath)

	if err == os.ErrNotExist {
		indexFile, _ := os.Create(indexPath)
		indexFile.Close()
	}

	files, err := ioutil.ReadDir(client.BaseDir)

	if err != nil {
		log.Println("error reading file info of the base directory", err.Error())
	}

	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)

	if err != nil {
		log.Println("error loading metadata from index.db", err.Error())
	}

	// sync local index
	hashes := make(map[string][]string) //so localMetaMap is information from index.db, and hashes is created by loopking up all local feils
	// we want to make sure hashes and localmetaMap.blockHashList contains the same information

	for _, file := range files {

		if file.Name() == "index.db" {
			continue
		}

		if int(float64(file.Size())) != 0 {
			numOfBlocks := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

			fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())

			if err != nil {
				log.Println("line 50 utils.go", err.Error())
			}

			for i := 0; i < numOfBlocks; i++ {

				blockData := make([]byte, client.BlockSize)

				n, err := fileToRead.Read(blockData)

				if err != nil {
					log.Println("line 60 utils.go, error filling blocks in file", err.Error())
				}

				blockData = blockData[:n]

				hash := GetBlockHashString(blockData)

				hashes[file.Name()] = append(hashes[file.Name()], hash)

			}
		} else { //means the current file is empty
			hashes[file.Name()] = append(hashes[file.Name()], "-1")
		}

		val, ok := localMetaMap[file.Name()]

		if !ok {
			meta := &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: hashes[file.Name()]}

			localMetaMap[file.Name()] = meta
		} else {
			if !equalArray(val.BlockHashList, hashes[file.Name()]) {
				localMetaMap[file.Name()].BlockHashList = hashes[file.Name()]
				localMetaMap[file.Name()].Version += 1
			}
		}

	}

	// fmt.Println("got here line 92")

	// check deleted files

	for fileName, metaData := range localMetaMap {

		_, ok := hashes[fileName]

		if !ok { // that means the file has been deleted in local directory
			if !(len(metaData.BlockHashList) == 1 && metaData.BlockHashList[0] == "0") {

				localMetaMap[fileName].Version += 1
				localMetaMap[fileName].BlockHashList = []string{"0"}

			}
		}
	}

	// var blockStoreAddr string

	// err2 := client.GetBlockStoreAddr(&blockStoreAddr)

	// if err2 != nil {
	// 	log.Println("error getting blockstoreAddr", err2.Error())
	// }

	// get metadata from server as well as the store address
	remoteMetaMap := make(map[string]*FileMetaData)

	err3 := client.GetFileInfoMap(&remoteMetaMap)

	if err3 != nil {
		fmt.Println("error getting index from server", err3.Error())
	}

	// upload files if any local files are updated, only upload when local version is greater than remote version
	for fileName, localData := range localMetaMap {

		remoteData, ok := remoteMetaMap[fileName]

		if ok {
			if localData.Version > remoteData.Version {

				fmt.Println("line 135 got here " + fileName)

				upload(client, localData)
			}
		} else { //means this file is in local directory but not in remote server
			upload(client, localData)
		}
	}

	// fmt.Println("got here line 144")

	// download files if any server files are updated

	// fmt.Println(remoteMetaMap)

	for fileName, remoteData := range remoteMetaMap {

		localData, ok := localMetaMap[fileName]

		if ok {
			if remoteData.Version > localData.Version {
				*localMetaMap[fileName], _ = download(client, remoteData)
			} else if remoteData.Version == localData.Version && !equalArray(remoteData.BlockHashList, localData.BlockHashList) {
				*localMetaMap[fileName], _ = download(client, remoteData)
			}
		} else {

			localMetaMap[fileName] = &FileMetaData{}

			// localData = localMetaMap[fileName]

			*localMetaMap[fileName], _ = download(client, remoteData)
		}
	}

	// fmt.Println("got here 168")

	WriteMetaFile(localMetaMap, client.BaseDir)

	// fmt.Println("got here 170")
}

func equalArray(a []string, b []string) bool {

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func upload(client RPCClient, metaData *FileMetaData) error {
	// panic("todo")

	filePath := client.BaseDir + "/" + metaData.Filename

	var latestVersion int32

	fileInfo, err := os.Stat(filePath)

	if err != nil {
		fmt.Println("line 194 err" + err.Error())
	}

	if os.IsNotExist(err) { // deleted file can still be in localMetaMap so could still need to be updated

		// fmt.Println("got here line 199")

		err2 := client.UpdateFile(metaData, &latestVersion)

		if err2 != nil {
			log.Println("error uploading file", err2.Error())
		}

		metaData.Version = latestVersion

		return err2
	}

	file, err := os.Open(filePath)

	if err != nil {
		fmt.Println("error opening file when trying to upload", err.Error())
	}

	defer file.Close()

	// 方案一，建立一个[block hash] *block 的map, 然后用blockStoreMap，loop throuth block server 进行putBlock

	hash_to_block := make(map[string]*Block)

	numOfBlocks := int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))

	for i := 0; i < numOfBlocks; i++ {

		blockData := make([]byte, client.BlockSize)

		n, err := file.Read(blockData)

		if err != nil {
			fmt.Println("line 233 utils.go, error filling blocks in file", err.Error())
		}

		blockData = blockData[:n]

		hash := GetBlockHashString(blockData)

		block := &Block{BlockData: blockData, BlockSize: int32(n)}

		hash_to_block[hash] = block

		// var success bool

		// err2 := client.PutBlock(block, blockStoreAddr, &success)

		// if err2 != nil {
		// 	log.Println("error in putBlock", err2)
		// }

	}

	blockStoreMap := make(map[string][]string)

	err2 := client.GetBlockStoreMap(metaData.BlockHashList, &blockStoreMap)

	if err2 != nil {
		fmt.Println("error getting blockStore map", err2.Error())
	}

	// fmt.Println(metaData.BlockHashList)

	// fmt.Println(metaData.Filename, blockStoreMap)

	for server, hashList := range blockStoreMap {

		for _, hash := range hashList {

			var success bool

			err3 := client.PutBlock(hash_to_block[hash], server, &success)

			if err3 != nil {
				fmt.Println("error in putBlock", err3.Error())
			}
		}

	}

	err4 := client.UpdateFile(metaData, &latestVersion)

	if err4 != nil {
		fmt.Println("error when updateFile (not a deleted file)", err4.Error())
		latestVersion = -1
	}

	metaData.Version = latestVersion

	return nil

}

func download(client RPCClient, remoteMetaData *FileMetaData) (FileMetaData, error) {
	// panic("todo")

	filePath := ConcatPath(client.BaseDir, remoteMetaData.Filename)

	file, err := os.Create(filePath)

	if err != nil {
		fmt.Println("failed to create new file", err.Error())
	}

	defer file.Close()
	updated_localMetaData := *remoteMetaData

	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" { // file is deleted in server

		err2 := os.Remove(filePath)

		if err2 != nil {
			fmt.Println("error removing a file that is deleted in server", err2.Error())
		}

		return updated_localMetaData, nil
	}

	// get blockstore map
	blockStoreMap := make(map[string][]string)

	err4 := client.GetBlockStoreMap(remoteMetaData.BlockHashList, &blockStoreMap)

	// fmt.Println(blockStoreMap)

	if err4 != nil {
		fmt.Println("error getting blockStore map", err4.Error())
	}

	hash_to_block := make(map[string]*Block)

	// get block from server

	for server, hashList := range blockStoreMap {

		for _, hash := range hashList {

			block := &Block{}

			err2 := client.GetBlock(hash, server, block)

			if err2 != nil {
				fmt.Println("error getting block from server", err2.Error())
			}

			// _, err3 := file.Write(block.BlockData)

			// if err3 != nil {
			// 	fmt.Println("error writing data to file", err3.Error())
			// }

			hash_to_block[hash] = block
		}
	}

	// fmt.Println(remoteMetaData.Filename, remoteMetaData.BlockHashList)

	// if remoteMetaData.Filename != ".DS_Store" {
	// 	fmt.Println(hash_to_block)
	// 	fmt.Println(remoteMetaData.BlockHashList)
	// }

	for _, hash := range remoteMetaData.BlockHashList {

		block, ok := hash_to_block[hash]

		if !ok {
			fmt.Println("some blocks are not pushed")
		}

		// fmt.Println(hash, block)

		_, err3 := file.Write(block.BlockData)

		if err3 != nil {
			fmt.Println("error writing data to file", err3.Error())
		}
	}

	// for _, hash := range remoteMetaData.BlockHashList {

	// 	block := &Block{}

	// 	err2 := client.GetBlock(hash, blockStoreAddr, block)

	// 	if err2 != nil {
	// 		log.Println("error when get block", err2.Error())
	// 	}

	// 	_, err3 := file.Write(block.BlockData)

	// 	if err3 != nil {
	// 		log.Println("error writing data into file")
	// 	}

	// }

	return updated_localMetaData, nil

}
