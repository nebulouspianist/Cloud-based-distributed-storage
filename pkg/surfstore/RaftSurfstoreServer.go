package surfstore

import (
	context "context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	id     int64
	ipList []string

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")

	s.isCrashedMutex.RLock()

	isCrashed := s.isCrashed

	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader

	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majoratiAlive, _ := s.SendHeartbeat(ctx, empty)

		if majoratiAlive.Flag {
			break
		}
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")

	s.isCrashedMutex.RLock()

	isCrashed := s.isCrashed

	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader

	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// for {
	// 	majoratiAlive, _ := s.SendHeartbeat(ctx, empty)

	// 	if majoratiAlive.Flag {
	// 		break
	// 	}
	// }

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
	// return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")

	s.isCrashedMutex.RLock()

	isCrashed := s.isCrashed

	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader

	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majoratiAlive, _ := s.SendHeartbeat(ctx, empty)

		if majoratiAlive.Flag {
			break
		}
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	// return nil, nil
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
// if a majority of the nodes are crashed, should block until a majority recover.
// If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")

	log.Println("set leader initialized")
	s.isCrashedMutex.RLock()

	isCrashed := s.isCrashed

	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader

	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	commitChan := make(chan bool)

	// s.pendingCommits indicates a list of updateOperation that needs to be commit, but will not necessarily be successful
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	go s.sendToAllFollowers()

	// success will be true if majority of the followers have the content in their log
	success := <-commitChan

	if success {
		// s.lastApplied = s.commitIndex
		s.lastApplied += 1
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, ERR_SERVER_CRASHED
}

func (s *RaftSurfstore) sendToAllFollowers() {
	// panic("todo")

	targetIndex := s.commitIndex + 1

	// should be the latest, so the last index in s.pendingCommits
	pendingIndex := int64(len(s.pendingCommits) - 1)

	commitChan := make(chan *AppendEntryOutput, len(s.ipList))

	for index, _ := range s.ipList {
		if int64(index) == s.id {
			continue
		}

		go s.sendToFollower(int64(index), targetIndex, commitChan)

	}

	commitCount := 1

	for {

		commit := <-commitChan

		s.isCrashedMutex.RLock()
		crashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		if crashed {
			*s.pendingCommits[pendingIndex] <- false
			break
		}

		if commit != nil && commit.Success {
			commitCount += 1
		}

		if commitCount >= int(math.Ceil(float64(len(s.ipList)/2))) {
			s.commitIndex = targetIndex

			*s.pendingCommits[pendingIndex] <- true
			break
		}

	}

}

func (s *RaftSurfstore) sendToFollower(serverIndex int64, entryIndex int64, commitChan chan *AppendEntryOutput) {
	// panic("todo")

	for {
		s.isCrashedMutex.RLock()
		crashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		if crashed {
			commitChan <- &AppendEntryOutput{Success: false}
		}

		addr := s.ipList[serverIndex]

		conn, err := grpc.Dial(addr, grpc.WithInsecure())

		if err != nil {
			conn.Close()
			return
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var prevLogTerm int64

		if entryIndex == 0 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[entryIndex-1].Term
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: entryIndex - 1,
			PrevLogTerm:  prevLogTerm,

			Entries:      s.log[:entryIndex+1],
			LeaderCommit: s.commitIndex,
		}

		output, _ := c.AppendEntries(ctx, input)

		if output != nil {
			if output.Success {
				commitChan <- output
				break

			}
		}

	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")

	s.isCrashedMutex.RLock()
	crashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	if crashed {
		return output, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		// defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		return output, nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm (§5.3)
	if len(input.Entries) != 0 {
		if input.PrevLogIndex >= 0 {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				return output, nil
			}
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)

	for index, logEntry := range s.log {

		s.lastApplied = int64(index - 1)

		if len(input.Entries) < index+1 {
			s.log = s.log[:index]
			input.Entries = make([]*UpdateOperation, 0)
			break
		}

		if logEntry != input.Entries[index] {
			s.log = s.log[:index]

			input.Entries = input.Entries[index:]
			break
		}

		// questionable, need to check again ??????????
		if len(s.log) == index+1 {
			input.Entries = input.Entries[index+1:]
		}
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}

	output.Success = true

	return output, nil

	// return nil, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")

	fmt.Println("set leader initialized")
	s.isCrashedMutex.RLock()

	isCrashed := s.isCrashed

	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.term++

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")

	s.isCrashedMutex.RLock()
	crashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if crashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	majorityAlive := false
	aliveCount := 1

	for index, addr := range s.ipList {

		if int64(index) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())

		if err != nil {
			conn.Close()
			return &Success{Flag: false}, err
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var prevLogTerm int64

		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		output, _ := c.AppendEntries(ctx, input)

		if output != nil {
			aliveCount += 1

			if aliveCount >= int(math.Ceil(float64(len(s.ipList)/2))) {
				majorityAlive = true
			}
		}
	}

	fmt.Println("send hearbeat end")
	return &Success{Flag: majorityAlive}, nil
	// return nil, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
