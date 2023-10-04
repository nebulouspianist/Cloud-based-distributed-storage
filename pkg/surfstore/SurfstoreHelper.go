package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	// fmt.Println("got here helper line 60")
	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println("got here helper line 63", err.Error())
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// panic("todo")

	// fmt.Println("got here helper line 67")

	statement2, err2 := db.Prepare(insertTuple)

	if err2 != nil {
		log.Println(err2.Error())
	}

	for _, fileData := range fileMetas {

		for i, v := range fileData.BlockHashList {

			hashIndex := i

			hashValue := v

			statement2.Exec(fileData.Filename, fileData.Version, int32(hashIndex), hashValue)

		}

		// hashIndex, _ := strconv.Atoi(fileData.BlockHashList[0])

		// statement2.Exec(fileData.Filename, fileData.Version, int32(hashIndex), fileData.BlockHashList[1])

	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select DISTINCT fileName, version from indexes`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue
										from indexes
										order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// panic("todo")

	blockListMap := make(map[string][]string)

	rows, err := db.Query(getTuplesByFileName)

	var fileName string
	var version int
	var hashIndex int
	var hashValue string

	for rows.Next() {
		rows.Scan(&fileName, &version, &hashIndex, &hashValue)
		// fmt.Println(fileName + ", " + strconv.Itoa(version) + ", " + strconv.Itoa(hashIndex) + ", " + hashValue)

		blockListMap[fileName] = append(blockListMap[fileName], hashValue)

		// temp := []string{strconv.Itoa(hashIndex), hashValue}

		// fileMetaMap[fileName] = &FileMetaData{
		// 	Filename:      fileName,
		// 	Version:       int32(version),
		// 	BlockHashList: temp,
		// }
	}

	rows2, err := db.Query(getDistinctFileName)

	fmt.Println("distinct fileName and version: ")

	for rows2.Next() {
		rows2.Scan(&fileName, &version)

		fmt.Println(fileName + " " + strconv.Itoa(version))

		fileMetaMap[fileName] = &FileMetaData{
			Filename:      fileName,
			Version:       int32(version),
			BlockHashList: blockListMap[fileName],
		}
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
