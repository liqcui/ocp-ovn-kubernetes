package cni

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

const (
	// LockDir is the directory for CNI concurrency control locks
	LockDir = "/var/run/ovn-kubernetes/cni"

	// MaxConcurrentCNI is the maximum number of concurrent CNI operations
	// Set to 300 to handle high pod churn while preventing resource exhaustion
	// This provides 20% more capacity than the original 250 limit
	MaxConcurrentCNI = 300

	// LockTimeout is how long to wait for lock acquisition
	// Extended to 90 seconds to prevent "resource temporarily unavailable" errors
	// during burst pod creation scenarios
	LockTimeout = 90 * time.Second

	// InitialRetryDelay is the starting delay between lock acquisition retries
	// Set to 1ms for fastest possible acquisition in low-contention scenarios
	InitialRetryDelay = 1 * time.Millisecond

	// MaxRetryDelay is the maximum delay between retries
	// Reduced to 30ms to retry more aggressively and acquire slots faster
	MaxRetryDelay = 30 * time.Millisecond

	// SamplingInterval for counting active slots (only count every N acquisitions)
	// Increased to 20 to reduce overhead with more slots
	SamplingInterval = 20

	// FastRetryThreshold is the number of full slot scans before switching to fast retry
	// After this many failed attempts, we retry with minimal delay to grab slots quickly
	FastRetryThreshold = 3
)

var (
	// acquisitionCounter tracks how many locks have been acquired
	// Used for sampling-based active slot counting to reduce overhead
	acquisitionCounter int
)

// CNILock represents a system-wide semaphore slot for CNI operations
type CNILock struct {
	file     *os.File
	slotPath string
	slotNum  int
}

// countActiveSlots returns the number of currently locked slots
// This is used for debugging to show current concurrency level
// TODO(debug): Remove after debugging is complete
func countActiveSlots() int {
	count := 0
	for slotNum := 0; slotNum < MaxConcurrentCNI; slotNum++ {
		slotPath := filepath.Join(LockDir, fmt.Sprintf("slot-%03d.lock", slotNum))

		// Try to open the slot file
		f, err := os.OpenFile(slotPath, os.O_RDONLY, 0644)
		if err != nil {
			// File doesn't exist yet, skip
			continue
		}

		// Try non-blocking lock to check if it's in use
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == syscall.EWOULDBLOCK {
			// Slot is locked (in use)
			count++
		} else if err == nil {
			// We got the lock, release it immediately
			syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}
		f.Close()
	}
	return count
}

// hashPID returns a hash of the PID to use as a starting slot offset
// This distributes processes across different slots to reduce contention
func hashPID(pid int) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d", pid)))
	return int(h.Sum32() % uint32(MaxConcurrentCNI))
}

// AcquireCNILock acquires one of the available semaphore slots for CNI operations
// This ensures controlled concurrency (max 250 concurrent) across all CNI processes
//
// Optimizations:
// 1. Start from hash-based offset to distribute load
// 2. Exponential backoff with jitter to reduce thundering herd
// 3. Sample-based active slot counting to reduce overhead
func AcquireCNILock() (*CNILock, error) {
	pid := os.Getpid()
	startTime := time.Now()

	// Ensure lock directory exists
	if err := os.MkdirAll(LockDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lock directory: %v", err)
	}

	klog.V(4).Infof("[CNI-DEBUG] Attempting to acquire semaphore slot: PID=%d, MaxSlots=%d, Time=%s",
		pid, MaxConcurrentCNI, startTime.Format(time.RFC3339Nano))

	deadline := time.Now().Add(LockTimeout)
	retryCount := 0
	retryDelay := InitialRetryDelay

	// Start from a hash-based offset to distribute processes across slots
	// This reduces contention on low-numbered slots
	startOffset := hashPID(pid)

	// Try to acquire any available slot
	for time.Now().Before(deadline) {
		// Optimization: On first few attempts, try a subset of slots for speed
		// This reduces time spent scanning all 300 slots when load is moderate
		slotsToTry := MaxConcurrentCNI
		if retryCount < FastRetryThreshold {
			// Try 100 slots on first few attempts for faster acquisition
			// This is enough to find a slot in most cases
			slotsToTry = 100
		}

		// Try each slot starting from the hash offset
		// This provides better distribution than always starting from slot 0
		for i := 0; i < slotsToTry; i++ {
			slotNum := (startOffset + i) % MaxConcurrentCNI
			slotPath := filepath.Join(LockDir, fmt.Sprintf("slot-%03d.lock", slotNum))

			// Try to open and lock this slot
			f, err := os.OpenFile(slotPath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				klog.Warningf("[CNI-DEBUG] Failed to open slot file: PID=%d, Slot=%d, Path=%s, Error=%v",
					pid, slotNum, slotPath, err)
				continue
			}

			// Try non-blocking exclusive lock
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				// Successfully acquired this slot
				acquireTime := time.Since(startTime)

				// Only count active slots periodically to reduce overhead
				acquisitionCounter++
				shouldCount := (acquisitionCounter % SamplingInterval) == 0
				activeSlots := -1
				if shouldCount {
					activeSlots = countActiveSlots()
				}

				if activeSlots >= 0 {
					klog.Infof("[CNI-DEBUG] Semaphore slot acquired: PID=%d, Slot=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d, Time=%s",
						pid, slotNum, activeSlots, MaxConcurrentCNI, acquireTime, retryCount, time.Now().Format(time.RFC3339Nano))
				} else {
					klog.Infof("[CNI-DEBUG] Semaphore slot acquired: PID=%d, Slot=%d, WaitTime=%v, Retries=%d, Time=%s",
						pid, slotNum, acquireTime, retryCount, time.Now().Format(time.RFC3339Nano))
				}

				return &CNILock{
					file:     f,
					slotPath: slotPath,
					slotNum:  slotNum,
				}, nil
			}

			// Lock acquisition failed, close file and try next slot
			f.Close()

			if err != syscall.EWOULDBLOCK {
				klog.Warningf("[CNI-DEBUG] Unexpected lock error: PID=%d, Slot=%d, Error=%v", pid, slotNum, err)
			}
		}

		// All slots are busy, increment retry counter and wait with adaptive backoff
		retryCount++
		if retryCount == 1 || retryCount%100 == 0 {
			// Log every 100th retry to show we're still trying
			waitTime := time.Since(startTime)
			activeSlots := countActiveSlots()
			klog.V(4).Infof("[CNI-DEBUG] All %d semaphore slots busy: PID=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d, Delay=%v",
				MaxConcurrentCNI, pid, activeSlots, MaxConcurrentCNI, waitTime, retryCount, retryDelay)
		}

		// Adaptive backoff strategy:
		// - First few attempts: minimal delay for fast acquisition
		// - After FastRetryThreshold: exponential backoff to reduce CPU usage
		var actualDelay time.Duration
		if retryCount <= FastRetryThreshold {
			// Fast retry mode: minimal delay to grab slots as soon as they're free
			// This optimizes for speed during high pod creation bursts
			actualDelay = InitialRetryDelay
		} else {
			// Exponential backoff with jitter to reduce thundering herd
			// Add jitter: delay ± 25%
			jitter := retryDelay / 4
			actualDelay = retryDelay - jitter + time.Duration(pid%int(jitter*2))

			// Increase delay for next iteration (exponential backoff)
			retryDelay = retryDelay * 2
			if retryDelay > MaxRetryDelay {
				retryDelay = MaxRetryDelay
			}
		}

		time.Sleep(actualDelay)
	}

	// Timeout - could not acquire any slot
	totalWaitTime := time.Since(startTime)
	klog.Warningf("[CNI-DEBUG] Semaphore acquisition timeout: PID=%d, MaxSlots=%d, WaitTime=%v, Retries=%d, Timeout=%v",
		pid, MaxConcurrentCNI, totalWaitTime, retryCount, LockTimeout)
	return nil, fmt.Errorf("timeout waiting for CNI semaphore slot after %v (all %d slots busy)", LockTimeout, MaxConcurrentCNI)
}

// Release releases the semaphore slot
// Optimized to only count active slots periodically to reduce overhead
func (l *CNILock) Release() error {
	if l.file == nil {
		return nil
	}

	pid := os.Getpid()
	klog.V(4).Infof("[CNI-DEBUG] Releasing semaphore slot: PID=%d, Slot=%d, Path=%s, Time=%s",
		pid, l.slotNum, l.slotPath, time.Now().Format(time.RFC3339Nano))

	// Release lock
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		klog.Warningf("[CNI-DEBUG] Failed to unlock slot: PID=%d, Slot=%d, Path=%s, Error=%v",
			pid, l.slotNum, l.slotPath, err)
	}

	// Close file
	if err := l.file.Close(); err != nil {
		klog.Warningf("[CNI-DEBUG] Failed to close slot file: PID=%d, Slot=%d, Path=%s, Error=%v",
			pid, l.slotNum, l.slotPath, err)
		return err
	}

	// Only count active slots periodically to reduce overhead
	// Most of the time we just release without the expensive counting operation
	shouldCount := (acquisitionCounter % SamplingInterval) == 0
	if shouldCount {
		activeSlots := countActiveSlots()
		klog.V(4).Infof("[CNI-DEBUG] Semaphore slot released: PID=%d, Slot=%d, ActiveSlots=%d/%d, Path=%s, Time=%s",
			pid, l.slotNum, activeSlots, MaxConcurrentCNI, l.slotPath, time.Now().Format(time.RFC3339Nano))
	} else {
		klog.V(4).Infof("[CNI-DEBUG] Semaphore slot released: PID=%d, Slot=%d, Path=%s, Time=%s",
			pid, l.slotNum, l.slotPath, time.Now().Format(time.RFC3339Nano))
	}
	l.file = nil

	return nil
}
