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

	// MaxConcurrentCNI is the maximum number of concurrent CNI client operations
	// Set to 300 to handle high pod churn while preventing resource exhaustion
	// Server-side limit (200) provides additional protection against OVS/API overload
	// The difference (300 client - 200 server) buffers bursty workloads
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

	// FastRetryThreshold is the number of full slot scans before switching to fast retry
	// After this many failed attempts, we retry with minimal delay to grab slots quickly
	FastRetryThreshold = 3

	// SlowAcquisitionThreshold defines when to log slow acquisitions
	// Slow acquisitions are always logged with active slot count for troubleshooting
	SlowAcquisitionThreshold = 5 * time.Second
)

// CNILock represents a system-wide semaphore slot for CNI operations
type CNILock struct {
	file     *os.File
	slotPath string
	slotNum  int
}

// countActiveSlots returns the number of currently locked slots
// Called only on errors/slow operations to provide troubleshooting information
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
// This ensures controlled concurrency (max 300 concurrent) across all CNI processes
//
// Optimizations:
// 1. Start from hash-based offset to distribute load
// 2. Exponential backoff with jitter to reduce thundering herd
// 3. Minimal logging - only errors and slow operations with active slot count
func AcquireCNILock() (*CNILock, error) {
	pid := os.Getpid()
	startTime := time.Now()

	// Ensure lock directory exists
	if err := os.MkdirAll(LockDir, 0755); err != nil {
		activeSlots := countActiveSlots()
		return nil, fmt.Errorf("failed to create lock directory (ActiveSlots=%d/%d): %v",
			activeSlots, MaxConcurrentCNI, err)
	}

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
				// Only log file open errors occasionally to avoid spam
				if retryCount%100 == 0 {
					activeSlots := countActiveSlots()
					klog.Warningf("Failed to open semaphore slot (ActiveSlots=%d/%d, Slot=%d, Retries=%d): %v",
						activeSlots, MaxConcurrentCNI, slotNum, retryCount, err)
				}
				continue
			}

			// Try non-blocking exclusive lock
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				// Successfully acquired this slot
				acquireTime := time.Since(startTime)

				// Always log if acquisition was abnormally slow (with active slot count)
				if acquireTime > SlowAcquisitionThreshold {
					activeSlots := countActiveSlots()
					klog.Warningf("Slow semaphore acquisition (ActiveSlots=%d/%d, Slot=%d, WaitTime=%v, Retries=%d)",
						activeSlots, MaxConcurrentCNI, slotNum, acquireTime, retryCount)
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
				klog.Warningf("Unexpected lock error: PID=%d, Slot=%d, Error=%v", pid, slotNum, err)
			}
		}

		// All slots are busy, increment retry counter and wait with adaptive backoff
		retryCount++

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
	activeSlots := countActiveSlots()
	klog.Warningf("Semaphore acquisition timeout (ActiveSlots=%d/%d, WaitTime=%v, Retries=%d)",
		activeSlots, MaxConcurrentCNI, totalWaitTime, retryCount)
	return nil, fmt.Errorf("timeout waiting for CNI semaphore slot after %v (ActiveSlots=%d/%d)",
		LockTimeout, activeSlots, MaxConcurrentCNI)
}

// Release releases the semaphore slot
func (l *CNILock) Release() error {
	if l.file == nil {
		return nil
	}

	// Release lock
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		klog.Warningf("Failed to unlock slot: Slot=%d, Error=%v",
			l.slotNum, err)
	}

	// Close file
	if err := l.file.Close(); err != nil {
		klog.Warningf("Failed to close slot file: Slot=%d, Error=%v",
			l.slotNum, err)
		return err
	}

	l.file = nil

	return nil
}
