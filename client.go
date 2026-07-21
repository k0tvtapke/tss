package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"tss/storage"

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

func (sc *StreamingClient) getClientStats() Stats {
	sc.stats.mu.RLock()
	defer sc.stats.mu.RUnlock()

	return Stats{Speed: sc.stats.Speed}
}

func (sc *StreamingClient) getTorrentsStats() map[torrent.InfoHash]Stats {
	stats := make(map[torrent.InfoHash]Stats)

	for infohash, entry := range sc.torrents {
		entry.stats.mu.RLock()
		stats[infohash] = Stats{Speed: entry.stats.Speed}
		entry.stats.mu.RUnlock()
	}

	return stats
}

// TODO убрать эту функцию после реализации геттера статистики
func (sc *StreamingClient) PrintStats() {
	sc.stats.mu.RLock()
	clientSpeed := sc.stats.Speed
	sc.stats.mu.RUnlock()

	log.Printf("Total speed: %5.2f Mbps; cache used: %d Mb\n", clientSpeed/1024/1024*8, sc.ramStorage.GetCurrentWatermark()/1024/1024)

	sc.mu.RLock()
	for infohash, entry := range sc.torrents {
		entry.stats.mu.RLock()
		entrySpeed := entry.stats.Speed
		entry.stats.mu.RUnlock()

		log.Printf("Torrent <%s> speed: %5.2f Mbps\n", infohash, entrySpeed/1024/1024*8)
	}
	sc.mu.RUnlock()
}

func main() {
	longMovieTorrentPath := "/home/k0tvtapke/Загрузки/Форма голоса Eiga Koe no Katachi A Silent Voice [Movie] [RUS(ext),JAP+Sub] [2016, драма, школа, BDRemux] [1080p] [rutracker-5405006](1).torrent"
	shortMovieMagnet := "magnet:?xt=urn:btih:815BCE419212917D9DA803450D765596C72BC2CC&tr=http%3A%2F%2Fbt4.t-ru.org%2Fann%3Fmagnet&dn=%D0%A4%D0%BE%D1%80%D0%BC%D0%B0%20%D0%B3%D0%BE%D0%BB%D0%BE%D1%81%D0%B0%20%2F%20Eiga%20Koe%20no%20Katachi%20%2F%20A%20Silent%20Voice%20%2F%20The%20Shape%20of%20Voice%20%5BMovie%5D%20%5BRUS(ext%2Fint)%2C%20JAP%2BSub%5D%20%5B2016%2C%20%D0%B4%D1%80%D0%B0%D0%BC%D0%B0%2C%20%D1%88%D0%BA%D0%BE%D0%BB%D0%B0%2C%20%D1%81%D1%91%D0%BD%D0%B5%D0%BD%2C%20BDRip%5D"
	longMovieMagnet := "magnet:?xt=urn:btih:94FB7200CE476654A66CD045FD1C98579FA3D400&tr=http%3A%2F%2Fbt.t-ru.org%2Fann%3Fmagnet&dn=%D0%A4%D0%BE%D1%80%D0%BC%D0%B0%20%D0%B3%D0%BE%D0%BB%D0%BE%D1%81%D0%B0%20%2F%20Eiga%20Koe%20no%20Katachi%20%2F%20A%20Silent%20Voice%20%5BMovie%5D%20%5BRUS(int)%2C%20JAP%2BSub%5D%20%5B2016%2C%20%D0%BF%D0%BE%D0%B2%D1%81%D0%B5%D0%B4%D0%BD%D0%B5%D0%B2%D0%BD%D0%BE%D1%81%D1%82%D1%8C%2C%20%D0%B4%D1%80%D0%B0%D0%BC%D0%B0%2C%20%D1%80%D0%BE%D0%BC%D0%B0%D0%BD%D1%82%D0%B8%D0%BA%D0%B0%2C%20BDRip%5D%20%5BHWP%5D"

	//if len(os.Args) != 2 {
	//	fmt.Println("Not enough arguments")
	//	fmt.Printf("Usage: %s <path>\n", os.Args[0])
	//	return
	//}

	config := StreamingClientConfig{
		ServerAddress:              "0.0.0.0",
		ServerPort:                 8080,
		DownloadPath:               "downloads",
		CacheHighWatermark:         1024 * 1024 * 1024,
		CacheLowWatermark:          768 * 1024 * 1024,
		ReadaheadSize:              128 * 1024 * 1024,
		StatsUpdateInterval:        time.Second,
		MagnetDataGatheringTimeout: time.Second * 30,
	}

	client := NewClient(config)
	fmt.Println(client.AddTorrentFromFile(longMovieTorrentPath, true, false))
	fmt.Println(client.AddTorrentFromFile(longMovieTorrentPath, true, false))   // Проверка на добавление уже имеющегося торрента
	fmt.Println(client.AddTorrentFromMagnet(longMovieTorrentPath, true, false)) // Проверка, сломается ли при неверных входных данных
	fmt.Println(client.AddTorrentFromMagnet(shortMovieMagnet, true, false))
	fmt.Println(client.AddTorrentFromMagnet(longMovieMagnet, true, false))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(config.StatsUpdateInterval)
	go func() {
		for range ticker.C {
			client.PrintStats()
		}
	}()

	<-quit
	ticker.Stop()

	client.Close()
}
