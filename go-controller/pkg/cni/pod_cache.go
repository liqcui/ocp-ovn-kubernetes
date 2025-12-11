package cni

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

const (
	// podCacheTTL is how long pod annotation entries remain valid
	// Short TTL ensures we don't serve stale data while still providing benefit during bursts
	podCacheTTL = 5 * time.Second

	// podCacheCleanupInterval is how often we remove expired entries
	podCacheCleanupInterval = 30 * time.Second

	// podCacheMaxSize limits cache memory usage
	podCacheMaxSize = 1000
)

type podCacheEntry struct {
	pod        *corev1.Pod
	annotation *util.PodAnnotation
	timestamp  time.Time
}

// PodCache caches pod annotations to reduce Kubernetes API calls during pod creation bursts
// This is safe because:
// 1. Cache TTL is short (5s) - pods don't change annotations frequently during creation
// 2. Cache is per-node - no cross-node consistency issues
// 3. Miss is handled gracefully - falls back to API call
type PodCache struct {
	mu     sync.RWMutex
	cache  map[string]*podCacheEntry // key: namespace/name
	stopCh chan struct{}
}

// NewPodCache creates a new pod annotation cache with automatic cleanup
func NewPodCache() *PodCache {
	pc := &PodCache{
		cache:  make(map[string]*podCacheEntry),
		stopCh: make(chan struct{}),
	}
	go pc.cleanupLoop()
	klog.Infof("Pod cache initialized with TTL=%v, cleanup interval=%v, max size=%d",
		podCacheTTL, podCacheCleanupInterval, podCacheMaxSize)
	return pc
}

// Get retrieves a cached pod and annotation if available and fresh
// Returns (pod, annotation, true) if found and fresh, (nil, nil, false) otherwise
func (pc *PodCache) Get(namespace, name string) (*corev1.Pod, *util.PodAnnotation, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	key := namespace + "/" + name
	entry, ok := pc.cache[key]
	if !ok {
		klog.V(5).Infof("Pod cache miss: %s", key)
		return nil, nil, false
	}

	// Check if entry is still fresh
	age := time.Since(entry.timestamp)
	if age > podCacheTTL {
		klog.V(5).Infof("Pod cache expired: %s (age=%v)", key, age)
		return nil, nil, false
	}

	klog.V(5).Infof("Pod cache hit: %s (age=%v)", key, age)
	return entry.pod, entry.annotation, true
}

// Set stores a pod and annotation in the cache
// If cache is full, oldest entries are evicted
func (pc *PodCache) Set(namespace, name string, pod *corev1.Pod, annotation *util.PodAnnotation) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Enforce max cache size by evicting oldest entries
	if len(pc.cache) >= podCacheMaxSize {
		pc.evictOldestLocked()
	}

	key := namespace + "/" + name
	pc.cache[key] = &podCacheEntry{
		pod:        pod,
		annotation: annotation,
		timestamp:  time.Now(),
	}
	klog.V(5).Infof("Pod cache set: %s (cache size=%d)", key, len(pc.cache))
}

// Invalidate removes an entry from the cache
// Used when we know a pod has been updated
func (pc *PodCache) Invalidate(namespace, name string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	key := namespace + "/" + name
	delete(pc.cache, key)
	klog.V(5).Infof("Pod cache invalidated: %s", key)
}

// evictOldestLocked removes the oldest 10% of entries
// Caller must hold pc.mu lock
func (pc *PodCache) evictOldestLocked() {
	if len(pc.cache) == 0 {
		return
	}

	// Find oldest 10% of entries
	toEvict := len(pc.cache) / 10
	if toEvict == 0 {
		toEvict = 1
	}

	// Collect all entries with timestamps
	type entryWithKey struct {
		key       string
		timestamp time.Time
	}
	entries := make([]entryWithKey, 0, len(pc.cache))
	for key, entry := range pc.cache {
		entries = append(entries, entryWithKey{key: key, timestamp: entry.timestamp})
	}

	// Sort by timestamp (oldest first)
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].timestamp.After(entries[j].timestamp) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Evict oldest entries
	evicted := 0
	for i := 0; i < toEvict && i < len(entries); i++ {
		delete(pc.cache, entries[i].key)
		evicted++
	}

	klog.V(4).Infof("Pod cache evicted %d entries (cache size now=%d)", evicted, len(pc.cache))
}

// cleanupLoop periodically removes expired entries
func (pc *PodCache) cleanupLoop() {
	ticker := time.NewTicker(podCacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pc.cleanup()
		case <-pc.stopCh:
			klog.Infof("Pod cache cleanup loop stopped")
			return
		}
	}
}

// cleanup removes all expired entries
func (pc *PodCache) cleanup() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	now := time.Now()
	removed := 0
	for key, entry := range pc.cache {
		if now.Sub(entry.timestamp) > podCacheTTL {
			delete(pc.cache, key)
			removed++
		}
	}

	if removed > 0 {
		klog.V(4).Infof("Pod cache cleanup removed %d expired entries (cache size now=%d)",
			removed, len(pc.cache))
	}
}

// Stop stops the cleanup loop
func (pc *PodCache) Stop() {
	close(pc.stopCh)
}

// Stats returns cache statistics for monitoring
func (pc *PodCache) Stats() map[string]interface{} {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return map[string]interface{}{
		"size":     len(pc.cache),
		"max_size": podCacheMaxSize,
		"ttl_seconds": podCacheTTL.Seconds(),
	}
}
