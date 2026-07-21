package storage

import (
	"container/list"
	"context"
	"io"
	"log"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

type lruKey struct {
	infoHash metainfo.Hash
	pieceIdx int
}

type lruElement struct {
	key lruKey
	ps  *pieceStorage
}

type RamStorage struct {
	mu       sync.RWMutex
	torrents map[metainfo.Hash]*torrentStorage

	highWatermark    int64
	lowWatermark     int64
	currentWatermark int64

	lruList     *list.List
	lruElements map[lruKey]*list.Element

	updateCompletionCallback func(infoHash metainfo.Hash, pieceIdx int)
}

type torrentStorage struct {
	mu     sync.Mutex
	pieces map[int]*pieceStorage

	updateLruCallback     func(pieceIdx int, ps *pieceStorage)
	removeFromLruCallback func()
}

type pieceStorage struct {
	mu       sync.RWMutex
	length   int64
	data     []byte
	complete bool

	updateLruCallback func()
}

func NewRamStorage(highWatermark, lowWatermark int64, updateCompletionCallback func(infoHash metainfo.Hash, pieceIdx int)) storage.ClientImpl {
	return &RamStorage{
		torrents:                 make(map[metainfo.Hash]*torrentStorage),
		highWatermark:            highWatermark,
		lowWatermark:             lowWatermark,
		lruList:                  list.New(),
		lruElements:              make(map[lruKey]*list.Element),
		updateCompletionCallback: updateCompletionCallback,
	}
}

func (rs *RamStorage) GetCurrentWatermark() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.currentWatermark
}

func (rs *RamStorage) updateLru(torrentHash metainfo.Hash, pieceIdx int, ps *pieceStorage) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	key := lruKey{
		infoHash: torrentHash,
		pieceIdx: pieceIdx,
	}

	if value, ok := rs.lruElements[key]; ok {
		rs.lruList.MoveToBack(value)
	} else {
		newLruElement := lruElement{
			key: key,
			ps:  ps,
		}
		newListEl := rs.lruList.PushBack(newLruElement)
		rs.lruElements[key] = newListEl

		rs.currentWatermark += ps.length

		if rs.currentWatermark > rs.highWatermark {
			log.Println("LRU cache cleanup...")
			for rs.currentWatermark > rs.lowWatermark {
				firstLruListElement := rs.lruList.Front()

				if firstLruListElement != nil {
					firstLruElement := firstLruListElement.Value.(lruElement)

					// Сначала очищаем память и помечаем блок как незавершенный
					firstLruElement.ps.Clear()

					rs.lruList.Remove(firstLruListElement)
					delete(rs.lruElements, firstLruElement.key)
					rs.currentWatermark -= firstLruElement.ps.length

					rs.updateCompletionCallback(firstLruElement.key.infoHash, firstLruElement.key.pieceIdx)
				} else {
					log.Panicln("LRU cache is empty, but free space is still not enough")
				}
			}
			go runtime.GC()
		}
	}
}

func (rs *RamStorage) removeAllLruEntriesByInfoHash(infoHash metainfo.Hash) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var next *list.Element
	for lruListEl := rs.lruList.Front(); lruListEl != nil; lruListEl = next {
		next = lruListEl.Next()
		lruEl := lruListEl.Value.(lruElement)

		if lruEl.key.infoHash == infoHash {
			lruEl.ps.Clear()
			rs.currentWatermark -= lruEl.ps.length

			rs.lruList.Remove(lruListEl)
			delete(rs.lruElements, lruEl.key)
		}
	}
	go runtime.GC()
}

func (rs *RamStorage) OpenTorrent(ctx context.Context, info *metainfo.Info, infoHash metainfo.Hash) (storage.TorrentImpl, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	ts, ok := rs.torrents[infoHash]
	if !ok {
		ts = &torrentStorage{
			pieces:                make(map[int]*pieceStorage),
			updateLruCallback:     func(pieceIdx int, ps *pieceStorage) { rs.updateLru(infoHash, pieceIdx, ps) },
			removeFromLruCallback: func() { rs.removeAllLruEntriesByInfoHash(infoHash) },
		}
		rs.torrents[infoHash] = ts
	}

	return storage.TorrentImpl{
		Piece: ts.Piece,
		Close: ts.Close,
	}, nil
}

func (ts *torrentStorage) Piece(piece metainfo.Piece) storage.PieceImpl {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	pieceIdx := piece.Index()
	ps, ok := ts.pieces[pieceIdx]
	if !ok {
		ps = &pieceStorage{
			length: piece.Length(),
		}
		ps.updateLruCallback = func() { ts.updateLruCallback(pieceIdx, ps) }
		ts.pieces[pieceIdx] = ps
	}

	return ps
}

func (ts *torrentStorage) Close() error {
	ts.mu.Lock()
	defer debug.FreeOSMemory()
	defer ts.mu.Unlock()

	ts.removeFromLruCallback()
	ts.pieces = nil

	return nil
}

func (ps *pieceStorage) ReadAt(b []byte, off int64) (int, error) {
	ps.mu.RLock()

	if ps.data == nil {
		ps.mu.RUnlock()
		return 0, io.ErrUnexpectedEOF
	}

	if off >= ps.length {
		ps.mu.RUnlock()
		return 0, io.EOF
	}

	n := copy(b, ps.data[off:])
	ps.mu.RUnlock()

	ps.updateLruCallback()

	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (ps *pieceStorage) WriteAt(b []byte, off int64) (int, error) {
	ps.mu.Lock()

	if ps.data == nil {
		ps.data = make([]byte, ps.length)
	}

	if off >= ps.length {
		ps.mu.Unlock()
		return 0, io.ErrShortWrite
	}

	n := copy(ps.data[off:], b)
	ps.mu.Unlock()

	ps.updateLruCallback()

	if n < len(b) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (ps *pieceStorage) MarkComplete() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.data != nil {
		ps.complete = true
	}
	return nil
}

func (ps *pieceStorage) MarkNotComplete() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.complete = false
	return nil
}

func (ps *pieceStorage) Completion() storage.Completion {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return storage.Completion{
		Ok:       true,
		Complete: ps.complete,
	}
}

func (ps *pieceStorage) Clear() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.data = nil
	ps.complete = false
}
