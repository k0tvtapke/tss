package client

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k0tvtapke/tss/storage"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
)

type StreamingClientConfig struct {
	ServerAddress string
	ServerPort    int
	ReadaheadSize int64

	CacheHighWatermark int64
	CacheLowWatermark  int64

	DownloadPath string

	StatsUpdateInterval        time.Duration
	MagnetDataGatheringTimeout time.Duration
}

type Stats struct {
	mu              sync.RWMutex
	Speed           float64
	prevUsefulBytes int64
}

type TorrentFile struct {
	path string
	size int64
}

type torrentEntry struct {
	torrent     *torrent.Torrent
	isStreaming bool
	stats       Stats
	isDropped   atomic.Bool
}

type StreamingClient struct {
	mu         sync.RWMutex
	client     *torrent.Client
	config     StreamingClientConfig
	ramStorage *storage.RamStorage
	stats      Stats
	torrents   map[metainfo.Hash]*torrentEntry
	server     *http.Server

	serverClosed    chan struct{}
	clientClosed    chan struct{}
	clientCloseOnce sync.Once
}

func NewClient(scConfig StreamingClientConfig) *StreamingClient {
	sc := &StreamingClient{
		torrents:     make(map[metainfo.Hash]*torrentEntry),
		config:       scConfig,
		serverClosed: make(chan struct{}),
		clientClosed: make(chan struct{}),
	}

	sc.ramStorage = storage.NewRamStorage(scConfig.CacheHighWatermark,
		scConfig.CacheLowWatermark,
		sc.updatePeaceCompletion).(*storage.RamStorage)

	config := torrent.NewDefaultClientConfig()
	config.DefaultStorage = sc.ramStorage
	config.DataDir = scConfig.DownloadPath

	client, err := torrent.NewClient(config)
	if err != nil {
		log.Panicf("Cannot create torrent client %v\n", err)
	}
	sc.client = client

	// TODO убедиться, действительно ли все нужное запускается при создании клиента
	sc.startUpdatingStats()
	sc.startServer()

	return sc
}

func (sc *StreamingClient) updatePeaceCompletion(infoHash metainfo.Hash, pieceIdx int) {
	sc.mu.RLock()
	entry, ok := sc.torrents[infoHash]
	sc.mu.RUnlock()

	if ok {
		entry.torrent.Piece(pieceIdx).UpdateCompletion()
	}
}

func (sc *StreamingClient) Close() {
	sc.clientCloseOnce.Do(func() {
		// Сначала завершаем сервер вне лока, чтоб не словить дедлок и охапку ошибок чтения или записи
		if err := sc.server.Close(); err != nil {
			log.Printf("Error during http server closing: %v", err)
		}
		<-sc.serverClosed

		sc.mu.Lock()
		close(sc.clientClosed)
		sc.client.Close()
		sc.ramStorage = nil // TODO проверить, очищается ли память без удаления ссылки на хранилище
		sc.mu.Unlock()

		<-sc.client.Closed()
	})
}

// downloadImmediately игнорируется при стриминге, в будущем предварительным скачиванием при стриминге будет заниматься отдельный метод
func (sc *StreamingClient) AddTorrentFromFile(path string, isStreaming, downloadImmediately bool) (err error, isNew bool, infohash torrent.InfoHash) {
	mi, err := metainfo.LoadFromFile(path)
	if err != nil {
		return
	}
	spec, err := torrent.TorrentSpecFromMetaInfoErr(mi)
	if err != nil {
		return
	}
	if isStreaming {
		spec.Storage = sc.ramStorage
	}
	t, isNew, err := sc.client.AddTorrentSpec(spec)
	if err != nil {
		return
	}
	infohash = t.InfoHash()
	if isNew {
		err = sc.handleNewTorrent(t, isStreaming, downloadImmediately)
	}

	return
}

// downloadImmediately игнорируется при стриминге, в будущем предварительным скачиванием при стриминге будет заниматься отдельный метод
func (sc *StreamingClient) AddTorrentFromMagnet(uri string, isStreaming, downloadImmediately bool) (err error, isNew bool, infohash torrent.InfoHash) {
	spec, err := torrent.TorrentSpecFromMagnetUri(uri)
	if err != nil {
		return
	}
	if isStreaming {
		spec.Storage = sc.ramStorage
	}
	t, isNew, err := sc.client.AddTorrentSpec(spec)
	if err != nil {
		return
	}
	infohash = t.InfoHash()
	if isNew {
		err = sc.handleNewTorrent(t, isStreaming, downloadImmediately)
	}

	return
}

func (sc *StreamingClient) handleNewTorrent(t *torrent.Torrent, isStreaming, downloadImmediately bool) (err error) {
	select {
	case <-t.GotInfo():
		// Метаданные получены, идем дальше
	case <-time.After(sc.config.MagnetDataGatheringTimeout):
		t.Drop()
		return errors.New("timeout waiting for torrent info")
	}

	sc.mu.Lock()
	sc.torrents[t.InfoHash()] = &torrentEntry{
		torrent:     t,
		isStreaming: isStreaming,
	}

	if downloadImmediately && !isStreaming {
		for _, file := range t.Files() {
			file.SetPriority(torrent.PiecePriorityReadahead)
		}
	} else {
		for _, file := range t.Files() {
			file.SetPriority(torrent.PiecePriorityNone)
		}
	}
	sc.mu.Unlock()

	return nil
}

func (sc *StreamingClient) RemoveTorrent(infohash torrent.InfoHash) error {
	sc.mu.Lock()
	t, ok := sc.torrents[infohash]
	if !ok {
		sc.mu.Unlock()
		return errors.New("torrent with given infohash not found")
	}
	t.isDropped.Store(true)
	sc.mu.Unlock()

	t.torrent.Drop()

	sc.mu.Lock()
	delete(sc.torrents, infohash)
	sc.mu.Unlock()
	return nil
}

func (sc *StreamingClient) GetFilesInTorrent(infohash torrent.InfoHash) (err error, torrentFiles []TorrentFile) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	t, ok := sc.torrents[infohash]
	if !ok {
		return errors.New("torrent with given infohash not found"), nil
	}

	for _, torrentFile := range t.torrent.Files() {
		torrentFiles = append(torrentFiles, TorrentFile{
			path: torrentFile.Path(),
			size: torrentFile.Length(),
		})
	}

	return
}

func (sc *StreamingClient) startUpdatingStats() {
	go func() {
		ticker := time.NewTicker(sc.config.StatsUpdateInterval)
		defer ticker.Stop()

		for range ticker.C {
			sc.mu.RLock()
			if len(sc.torrents) == 0 {
				sc.mu.RUnlock()
				continue
			}

			select {
			case <-sc.clientClosed:
				sc.mu.RUnlock()
				return
			default:
				stats := sc.client.Stats()
				currentUsefulBytes := stats.BytesReadUsefulData.Int64()

				sc.stats.mu.Lock()
				sc.stats.Speed = float64(currentUsefulBytes-sc.stats.prevUsefulBytes) / (float64(sc.config.StatsUpdateInterval) / float64(time.Second))
				sc.stats.prevUsefulBytes = currentUsefulBytes
				sc.stats.mu.Unlock()

				for _, entry := range sc.torrents {
					if entry.isDropped.Load() {
						continue
					}
					stats := entry.torrent.Stats()
					currentUsefulBytes := stats.BytesReadUsefulData.Int64()

					entry.stats.mu.Lock()
					entry.stats.Speed = float64(currentUsefulBytes-entry.stats.prevUsefulBytes) / (float64(sc.config.StatsUpdateInterval) / float64(time.Second))
					entry.stats.prevUsefulBytes = currentUsefulBytes
					entry.stats.mu.Unlock()
				}

				sc.mu.RUnlock()
			}
		}
	}()
}

func (sc *StreamingClient) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestPath := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(requestPath, "/", 2)

	switch {
	case requestPath == "":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintln(w, "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><title>Document</title></head><body>")

		sc.mu.RLock()
		for infoHash, entry := range sc.torrents {
			_, _ = fmt.Fprintf(w, "<h1>\n%s <%s>\n</h1>", entry.torrent.Name(), infoHash.HexString())
			for _, f := range entry.torrent.Files() {
				escapedPath := url.PathEscape(f.Path())
				fileUrl := fmt.Sprintf("<a href=\"http://%s/%s/%s\"><p>%s</p></a>", r.Host, infoHash.HexString(), escapedPath, f.Path())
				_, _ = fmt.Fprintln(w, fileUrl)
			}
		}
		sc.mu.RUnlock()
		_, _ = fmt.Fprintf(w, "</body></html>")
	case len(parts) == 2:
		infoHashStr := parts[0]
		filePath := parts[1]

		var infoHash metainfo.Hash
		if err := infoHash.FromHexString(infoHashStr); err != nil {
			http.Error(w, "Invalid infohash", http.StatusBadRequest)
			return
		}

		sc.mu.RLock()
		entry, exists := sc.torrents[infoHash]
		sc.mu.RUnlock()
		if !exists || entry.isDropped.Load() {
			http.NotFound(w, r)
			return
		}

		var targetFile *torrent.File
		for _, f := range entry.torrent.Files() {
			if path.Clean(f.Path()) == path.Clean(filePath) {
				targetFile = f
				break
			}
		}
		if targetFile == nil {
			http.NotFound(w, r)
			return
		}

		reader := targetFile.NewReader()
		defer func() { _ = reader.Close() }()

		reader.SetReadahead(sc.config.ReadaheadSize)
		fileName := filepath.Base(targetFile.DisplayPath())
		http.ServeContent(w, r, fileName, time.Time{}, reader)
	default:
		http.Error(w, "Invalid URL format. Use: \"/<infohash>/<filepath>\", or just \"/\"", http.StatusBadRequest)
	}
}

// TODO переделать HTTP сервер ПОЛНОСТЬЮ
func (sc *StreamingClient) startServer() {

	serverUrl := fmt.Sprintf(
		"%s:%d",
		sc.config.ServerAddress,
		sc.config.ServerPort,
	)

	sc.server = &http.Server{
		Addr:    serverUrl,
		Handler: sc,
	}

	go func() {
		log.Printf("Streaming started on http://%s/\n", serverUrl)
		if err := sc.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v\n", err)
		}
		log.Println("HTTP server closed")
		close(sc.serverClosed)
	}()
}

func (sc *StreamingClient) GetCacheStats() (used, total int64) {
	used = sc.ramStorage.GetCurrentWatermark()
	total = sc.config.CacheHighWatermark
	return
}

func (sc *StreamingClient) GetClientSpeed() float64 {
	sc.stats.mu.RLock()
	defer sc.stats.mu.RUnlock()

	return sc.stats.Speed
}

func (sc *StreamingClient) GetTorrentsSpeed() map[torrent.InfoHash]float64 {
	speeds := make(map[torrent.InfoHash]float64)

	for infohash, entry := range sc.torrents {
		entry.stats.mu.RLock()
		speeds[infohash] = entry.stats.Speed
		entry.stats.mu.RUnlock()
	}

	return speeds
}
