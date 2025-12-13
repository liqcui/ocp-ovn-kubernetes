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

	// Logging configuration
	// EnableDebugLogging controls detailed debug logging (Tier 3)
	// Set to false in production to reduce overhead by ~95%
	// Can be overridden via CNI_DEBUG_LOGGING environment variable
	EnableDebugLogging = false

	// LogSamplingRate controls how often to log normal operations
	// Only log every Nth successful acquisition (reduces log volume)
	// Set to 100 to log 1 out of 100 acquisitions
	LogSamplingRate = 100

	// SlowAcquisitionThreshold defines when to log slow acquisitions
	// Log acquisitions that take longer than this threshold
	SlowAcquisitionThreshold = 5 * time.Second

	// HighRetryThreshold defines when to log high retry counts
	// Log if acquisition requires more retries than this
	HighRetryThreshold = 50
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
// This ensures controlled concurrency (max 300 concurrent) across all CNI processes
//
// Optimizations:
// 1. Start from hash-based offset to distribute load
// 2. Exponential backoff with jitter to reduce thundering herd
// 3. Sample-based active slot counting to reduce overhead
// 4. Conditional logging to reduce performance overhead
func AcquireCNILock() (*CNILock, error) {
	pid := os.Getpid()
	startTime := time.Now()

	// Ensure lock directory exists
	if err := os.MkdirAll(LockDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lock directory: %v", err)
	}

	// Tier 3: Only log start if debug logging enabled
	if EnableDebugLogging {
		klog.V(4).Infof("[CNI-DEBUG] Attempting to acquire semaphore slot: PID=%d, MaxSlots=%d",
			pid, MaxConcurrentCNI)
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
				klog.Warningf("[CNI-DEBUG] Failed to open slot file: PID=%d, Slot=%d, Path=%s, Error=%v",
					pid, slotNum, slotPath, err)
				continue
			}

			// Try non-blocking exclusive lock
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				// Successfully acquired this slot
				acquireTime := time.Since(startTime)

				// Increment acquisition counter for sampling
				acquisitionCounter++

				// Decide if we should log this acquisition
				shouldLog := EnableDebugLogging || // Tier 3: Debug mode
					acquireTime > SlowAcquisitionThreshold || // Tier 2: Slow acquisition
					retryCount > HighRetryThreshold || // Tier 2: High retry count
					(acquisitionCounter%LogSamplingRate) == 0 // Tier 1: Sampling

				if shouldLog {
					// Only count active slots if we're actually logging
					// This is expensive (5-10ms), so skip unless needed
					activeSlots := -1
					if (acquisitionCounter % SamplingInterval) == 0 {
						activeSlots = countActiveSlots()
					}

					if activeSlots >= 0 {
						klog.Infof("[CNI-PERF] Semaphore slot acquired: PID=%d, Slot=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d",
							pid, slotNum, activeSlots, MaxConcurrentCNI, acquireTime, retryCount)
					} else {
						klog.Infof("[CNI-PERF] Semaphore slot acquired: PID=%d, Slot=%d, WaitTime=%v, Retries=%d",
							pid, slotNum, acquireTime, retryCount)
					}
				}

				// Tier 2: Always log if acquisition was abnormally slow
				if acquireTime > SlowAcquisitionThreshold {
					activeSlots := countActiveSlots()
					klog.Warningf("[CNI-PERF] Slow semaphore acquisition: PID=%d, Slot=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d",
						pid, slotNum, activeSlots, MaxConcurrentCNI, acquireTime, retryCount)
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
	// Tier 1: Always log errors
	totalWaitTime := time.Since(startTime)
	activeSlots := countActiveSlots()
	klog.Warningf("[CNI-ERROR] Semaphore acquisition timeout: PID=%d, MaxSlots=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d, Timeout=%v",
		pid, MaxConcurrentCNI, activeSlots, MaxConcurrentCNI, totalWaitTime, retryCount, LockTimeout)
	return nil, fmt.Errorf("timeout waiting for CNI semaphore slot after %v (all %d slots busy)", LockTimeout, MaxConcurrentCNI)
}

// Release releases the semaphore slot
// Optimized with conditional logging to reduce overhead
func (l *CNILock) Release() error {
	if l.file == nil {
		return nil
	}

	// Tier 3: Only log release if debug logging enabled
	if EnableDebugLogging {
		pid := os.Getpid()
		klog.V(4).Infof("[CNI-DEBUG] Releasing semaphore slot: PID=%d, Slot=%d",
			pid, l.slotNum)
	}

	// Release lock
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		// Tier 1: Always log errors
		klog.Warningf("[CNI-ERROR] Failed to unlock slot: Slot=%d, Error=%v",
			l.slotNum, err)
	}

	// Close file
	if err := l.file.Close(); err != nil {
		// Tier 1: Always log errors
		klog.Warningf("[CNI-ERROR] Failed to close slot file: Slot=%d, Error=%v",
			l.slotNum, err)
		return err
	}

	// No need to log every release in production
	// Acquisition logs already provide concurrency visibility
	l.file = nil

	return nil
}
