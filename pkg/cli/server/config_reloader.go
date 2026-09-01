package server

import (
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
	"zotregistry.dev/zot/v2/pkg/log"
)

const (
	// configEventDebounceInterval coalesces multiple fsnotify events into one reload.
	// Kubernetes Secret/ConfigMap updates often emit Remove/Create/Rename bursts.
	configEventDebounceInterval = 150 * time.Millisecond
	// configStatCheckInterval is the polling interval used as a backstop when
	// inotify watching is unavailable or events were dropped.
	configStatCheckInterval = 1 * time.Second
)

// HotReloader reloads the server configuration when the config file (or the
// LDAP credentials file it references) changes.
//
// Mirrors HTPasswdWatcher behavior for Kubernetes Secret/ConfigMap mounts that
// replace files via atomic rename/symlink swap (Remove/Create/Rename events on
// the parent directory, never a Write on the watched file): the file and its
// parent directory are watched as a unit, events are debounced, and a periodic
// stat fingerprint check backstops dropped or unavailable inotify watches.
type HotReloader struct {
	watcher             *fsnotify.Watcher
	configPath          string
	ldapCredentialsPath string
	ctlr                *api.Controller
	logger              log.Logger

	// mu protects the fields below. Lock sections are short and never wait on
	// channels or timers, so the mutex cannot participate in a deadlock cycle.
	mu            sync.Mutex
	debounceTimer *time.Timer
	// configInfo / ldapInfo are stat fingerprints of the files at the last
	// successful reload; polling compares identity (os.SameFile), size and
	// mtime against them, so atomic replacements are detected even when
	// inotify saw nothing.
	configInfo os.FileInfo
	ldapInfo   os.FileInfo

	done     chan struct{}
	stopOnce sync.Once
}

func NewHotReloader(ctlr *api.Controller, filePath, ldapCredentialsPath string) (*HotReloader, error) {
	// creates a new file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	hotReloader := &HotReloader{
		watcher:             watcher,
		configPath:          filePath,
		ldapCredentialsPath: ldapCredentialsPath,
		ctlr:                ctlr,
		logger:              log.NewLogger("info", ""),
		done:                make(chan struct{}),
	}

	return hotReloader, nil
}

func signalHandler(ctlr *api.Controller, hr *HotReloader, sigCh chan os.Signal) {
	// if signal then shutdown
	if sig, ok := <-sigCh; ok {
		ctlr.Log.Info().Interface("signal", sig).Msg("received signal")

		hr.Stop()
		// gracefully shutdown http server
		ctlr.Shutdown() //nolint: contextcheck
	}
}

func initShutDownRoutine(ctlr *api.Controller, hr *HotReloader) {
	sigCh := make(chan os.Signal, 1)

	go signalHandler(ctlr, hr, sigCh)

	// block all async signals to this server
	signal.Ignore()

	// handle SIGINT and SIGHUP.
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
}

func (hr *HotReloader) Stop() {
	hr.stopOnce.Do(func() {
		if hr.done != nil {
			close(hr.done)
		}
		if hr.watcher != nil {
			_ = hr.watcher.Close()
		}
	})
}

func (hr *HotReloader) Start() {
	// Watch failures are tolerated: the loop always fingerprint-polls as a
	// backstop, so the reloader works (more slowly) without inotify.
	if err := addWatches(hr.watcher, hr.configPath); err != nil {
		hr.logger.Error().Err(err).Str("config", hr.configPath).
			Msg("failed to add config file to fsnotify watcher, relying on stat-based polling")
	}

	if hr.ldapCredentialsPath != "" {
		if err := addWatches(hr.watcher, hr.ldapCredentialsPath); err != nil {
			hr.logger.Error().Err(err).Str("ldap-credentials", hr.ldapCredentialsPath).
				Msg("failed to add ldap-credentials to fsnotify watcher, relying on stat-based polling")
		}
	}

	// Baseline fingerprints for the configuration already loaded at startup,
	// so the first poll tick does not trigger a spurious reload.
	hr.mu.Lock()
	hr.configInfo = statOrNil(hr.configPath)
	hr.ldapInfo = statOrNil(hr.ldapCredentialsPath)
	hr.mu.Unlock()

	go hr.loop()
}

func (hr *HotReloader) loop() {
	statTicker := time.NewTicker(configStatCheckInterval)
	defer statTicker.Stop()

	for {
		select {
		case <-hr.done:
			return

		// watch for events
		case event, ok := <-hr.watcher.Events:
			if !ok {
				return
			}

			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) == 0 {
				continue
			}

			if !eventAffectsWatchedFile(event.Name, hr.configPath) &&
				!eventAffectsWatchedFile(event.Name, hr.ldapCredentialsPath) {
				continue
			}

			// Atomic replacements retire the watched inode; re-add so the next
			// change is seen too. Polling remains the backstop on failure.
			if event.Op&(fsnotify.Remove|fsnotify.Rename|fsnotify.Create) != 0 {
				hr.readdWatches()
			}

			hr.scheduleReload()

		case <-hr.getDebounceChannel():
			hr.mu.Lock()
			hr.debounceTimer = nil
			hr.mu.Unlock()

			select {
			case <-hr.done:
				return
			default:
			}

			hr.reloadConfig("debounced file change")

		case <-statTicker.C:
			// Always fingerprint-poll, even when inotify is healthy: that
			// closes the gap when watches were lost on an atomic replacement
			// and could not be re-added in time.
			if hr.checkFilesChanged() {
				hr.readdWatches()
				hr.reloadConfig("stat-based polling")
			}

		// watch for errors
		case err, ok := <-hr.watcher.Errors:
			if !ok {
				return
			}

			hr.logger.Error().Err(err).Str("config", hr.configPath).Msg("fsnotify error while watching config")

			// Events may have been dropped (e.g. inotify queue overflow);
			// resynchronize sooner than the next poll tick.
			hr.scheduleReload()
		}
	}
}

func (hr *HotReloader) reloadConfig(reason string) {
	hr.logger.Info().Str("config", hr.configPath).Str("reason", reason).
		Msg("config file changed, trying to reload config")

	// Sample the fingerprints before reading the files: if a replacement lands
	// mid-read, the stale baseline makes the next poll detect and reload it.
	configInfo := statOrNil(hr.configPath)
	ldapInfo := statOrNil(hr.ldapCredentialsPath)

	newConfig := config.New()

	err := LoadConfiguration(newConfig, hr.configPath)
	if err != nil {
		hr.logger.Error().Err(err).Msg("failed to reload config, retry writing it.")

		return
	}

	authConfig := hr.ctlr.Config.CopyAuthConfig()
	if authConfig.IsLdapAuthEnabled() &&
		authConfig.LDAP.CredentialsFile != newConfig.HTTP.Auth.LDAP.CredentialsFile {
		err = removeWatches(hr.watcher, authConfig.LDAP.CredentialsFile)
		if err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
			hr.logger.Error().Err(err).Msg("failed to remove old watch for the credentials file")
		}

		hr.ldapCredentialsPath = newConfig.HTTP.Auth.LDAP.CredentialsFile

		if err := addWatches(hr.watcher, hr.ldapCredentialsPath); err != nil {
			hr.logger.Error().Err(err).Str("ldap-credentials-file", hr.ldapCredentialsPath).
				Msg("failed to watch ldap credentials file, relying on stat-based polling")
		}

		ldapInfo = statOrNil(hr.ldapCredentialsPath)
	}

	// stop background tasks gracefully
	hr.ctlr.StopBackgroundTasks()

	// load new config
	hr.ctlr.LoadNewConfig(newConfig)

	// start background tasks based on new loaded config
	hr.ctlr.StartBackgroundTasks()

	hr.mu.Lock()
	hr.configInfo = configInfo
	hr.ldapInfo = ldapInfo
	hr.mu.Unlock()
}

// checkFilesChanged reports whether the config or LDAP credentials file differs
// from the baseline fingerprint recorded at the last successful reload.
// Identity (os.SameFile) catches atomic-rename replacements even when the new
// file preserves the old timestamp; size catches same-inode rewrites within one
// coarse-timestamp granule; mtime catches plain in-place edits.
func (hr *HotReloader) checkFilesChanged() bool {
	hr.mu.Lock()
	configInfo, ldapInfo := hr.configInfo, hr.ldapInfo
	ldapPath := hr.ldapCredentialsPath
	hr.mu.Unlock()

	return fileChanged(hr.configPath, configInfo) || fileChanged(ldapPath, ldapInfo)
}

func fileChanged(path string, prev os.FileInfo) bool {
	if path == "" {
		return false
	}

	info, err := os.Stat(path)
	if err != nil {
		// Transient during atomic replacements; the next tick sees the new file.
		return false
	}

	if prev == nil {
		return true
	}

	return !os.SameFile(prev, info) || prev.Size() != info.Size() || !prev.ModTime().Equal(info.ModTime())
}

func (hr *HotReloader) getDebounceChannel() <-chan time.Time {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.debounceTimer == nil {
		return nil
	}

	return hr.debounceTimer.C
}

func (hr *HotReloader) scheduleReload() {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.debounceTimer != nil {
		if !hr.debounceTimer.Stop() {
			select {
			case <-hr.debounceTimer.C:
			default:
			}
		}
		hr.debounceTimer.Reset(configEventDebounceInterval)

		return
	}

	hr.debounceTimer = time.NewTimer(configEventDebounceInterval)
}

func (hr *HotReloader) readdWatches() {
	if err := addWatches(hr.watcher, hr.configPath); err != nil {
		hr.logger.Debug().Err(err).Str("config", hr.configPath).
			Msg("failed to re-add config watch, relying on stat-based polling")
	}

	hr.mu.Lock()
	ldapPath := hr.ldapCredentialsPath
	hr.mu.Unlock()

	if ldapPath != "" {
		if err := addWatches(hr.watcher, ldapPath); err != nil {
			hr.logger.Debug().Err(err).Str("ldap-credentials", ldapPath).
				Msg("failed to re-add ldap-credentials watch, relying on stat-based polling")
		}
	}
}

// addWatches watches filePath and its parent directory as a unit. The parent
// directory watch is required to detect Kubernetes Secret/ConfigMap updates,
// which atomically retarget the ..data symlink instead of writing to the
// watched inode. Adding an already-watched path is a no-op for fsnotify.
func addWatches(watcher *fsnotify.Watcher, filePath string) error {
	if err := watcher.Add(filePath); err != nil {
		return err
	}

	return watcher.Add(filepath.Dir(filePath))
}

// removeWatches drops the file watch; the parent directory watch is left in
// place since it may be shared with another watched file in the same mount.
func removeWatches(watcher *fsnotify.Watcher, filePath string) error {
	return watcher.Remove(filePath)
}

// eventAffectsWatchedFile reports whether a fsnotify event is relevant for the
// watched file. Since the parent directory is watched too, events for
// unrelated sibling files must be filtered out: only the exact file path and
// Kubernetes ..data symlink swaps in the same directory count.
func eventAffectsWatchedFile(eventName, filePath string) bool {
	if eventName == "" || filePath == "" {
		return false
	}

	eventName = filepath.Clean(eventName)
	filePath = filepath.Clean(filePath)

	if eventName == filePath {
		return true
	}

	// Kubernetes mounts swap the ..data (or ..data_tmp) symlink under the mount directory.
	if strings.HasPrefix(filepath.Base(eventName), "..data") {
		return filepath.Dir(eventName) == filepath.Dir(filePath)
	}

	return false
}

func statOrNil(path string) os.FileInfo {
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil
	}

	return info
}
