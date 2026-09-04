package events

import (
	"sync"
)

// ReloadableRecorder is a Recorder whose delegate can be replaced while the
// server runs. The image stores and the CVE scanner are handed this wrapper
// once, so swapping the delegate reaches all of them without touching any
// emit site. A nil delegate records nothing, which is how a build or a
// configuration without events behaves.
type ReloadableRecorder struct {
	// held for reading across the whole forwarded call, so once Swap returns
	// nothing can still enter the delegate it handed back and the caller is
	// free to close it.
	mu       sync.RWMutex
	recorder Recorder
}

var _ Recorder = (*ReloadableRecorder)(nil)

func NewReloadableRecorder(recorder Recorder) *ReloadableRecorder {
	return &ReloadableRecorder{recorder: recorder}
}

// Swap installs recorder and returns the delegate it replaced, which the caller
// closes once it is done with it. It waits for calls already inside the old
// delegate, so closing it cannot pull sinks out from under them.
func (r *ReloadableRecorder) Swap(recorder Recorder) Recorder {
	r.mu.Lock()
	defer r.mu.Unlock()

	previous := r.recorder
	r.recorder = recorder

	return previous
}

// with runs record against the installed delegate, if there is one. A nil
// wrapper records nothing, so a typed nil handed out as a Recorder is safe.
func (r *ReloadableRecorder) with(record func(recorder Recorder)) {
	if r == nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.recorder != nil {
		record(r.recorder)
	}
}

// Close detaches the delegate before closing it. Detaching takes the write
// lock, so a call already inside the delegate has returned, and registered the
// publish it dispatched, before its sinks are torn down. Closing through a read
// lock would let the two run together and close the sinks first.
func (r *ReloadableRecorder) Close() {
	if r == nil {
		return
	}

	if recorder := r.Swap(nil); recorder != nil {
		recorder.Close()
	}
}

func (r *ReloadableRecorder) RepositoryCreated(name string, ectx *EventContext) {
	r.with(func(recorder Recorder) { recorder.RepositoryCreated(name, ectx) })
}

func (r *ReloadableRecorder) ImageUpdated(name, reference, digest, mediaType, manifest string, ectx *EventContext) {
	r.with(func(recorder Recorder) {
		recorder.ImageUpdated(name, reference, digest, mediaType, manifest, ectx)
	})
}

func (r *ReloadableRecorder) ImageDeleted(name, reference, digest, mediaType string, ectx *EventContext) {
	r.with(func(recorder Recorder) {
		recorder.ImageDeleted(name, reference, digest, mediaType, ectx)
	})
}

func (r *ReloadableRecorder) ImageLintFailed(name, reference, digest, mediaType, manifest string, ectx *EventContext) {
	r.with(func(recorder Recorder) {
		recorder.ImageLintFailed(name, reference, digest, mediaType, manifest, ectx)
	})
}

func (r *ReloadableRecorder) ImageScanned(name, reference, digest, mediaType string,
	summary ImageScanSummary, ectx *EventContext,
) {
	r.with(func(recorder Recorder) {
		recorder.ImageScanned(name, reference, digest, mediaType, summary, ectx)
	})
}
