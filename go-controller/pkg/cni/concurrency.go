package cni

import (
	"fmt"
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
	// Reduced from 250 to 100 to balance with server capacity (MaxConcurrentServerRequests=100)
	// This prevents queue buildup and reduces resource contention
	MaxConcurrentCNI = 100

	// LockTimeout is how long to wait for lock acquisition
	LockTimeout = 60 * time.Second

	// RetryDelay is how long to wait between lock acquisition retries
	RetryDelay = 10 * time.Millisecond
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

// AcquireCNILock acquires one of the available semaphore slots for CNI operations
// This ensures controlled concurrency (max 250 concurrent) across all CNI processes
// TODO(debug): Remove detailed logging after debugging is complete
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

	// Try to acquire any available slot
	for time.Now().Before(deadline) {
		// Try each slot in order
		for slotNum := 0; slotNum < MaxConcurrentCNI; slotNum++ {
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
				activeSlots := countActiveSlots()
				klog.Infof("[CNI-DEBUG] Semaphore slot acquired: PID=%d, Slot=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d, Time=%s",
					pid, slotNum, activeSlots, MaxConcurrentCNI, acquireTime, retryCount, time.Now().Format(time.RFC3339Nano))

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

		// All slots are busy, increment retry counter and wait
		retryCount++
		if retryCount == 1 || retryCount%100 == 0 {
			// Log every 100th retry (every 1 second)
			waitTime := time.Since(startTime)
			activeSlots := countActiveSlots()
			klog.V(4).Infof("[CNI-DEBUG] All %d semaphore slots busy: PID=%d, ActiveSlots=%d/%d, WaitTime=%v, Retries=%d",
				MaxConcurrentCNI, pid, activeSlots, MaxConcurrentCNI, waitTime, retryCount)
		}

		// Wait before retrying
		time.Sleep(RetryDelay)
	}

	// Timeout - could not acquire any slot
	totalWaitTime := time.Since(startTime)
	klog.Warningf("[CNI-DEBUG] Semaphore acquisition timeout: PID=%d, MaxSlots=%d, WaitTime=%v, Retries=%d, Timeout=%v",
		pid, MaxConcurrentCNI, totalWaitTime, retryCount, LockTimeout)
	return nil, fmt.Errorf("timeout waiting for CNI semaphore slot after %v (all %d slots busy)", LockTimeout, MaxConcurrentCNI)
}

// Release releases the semaphore slot
// TODO(debug): Remove detailed logging after debugging is complete
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

	activeSlots := countActiveSlots()
	klog.V(4).Infof("[CNI-DEBUG] Semaphore slot released: PID=%d, Slot=%d, ActiveSlots=%d/%d, Path=%s, Time=%s",
		pid, l.slotNum, activeSlots, MaxConcurrentCNI, l.slotPath, time.Now().Format(time.RFC3339Nano))
	l.file = nil

	return nil
}
