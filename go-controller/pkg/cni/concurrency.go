package cni

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

const (
	// LockDir is the directory for CNI concurrency control locks
	LockDir = "/var/run/ovn-kubernetes/cni"

	// SerialLockFile ensures only one CNI operation runs at a time
	SerialLockFile = "/var/run/ovn-kubernetes/cni/serial.lock"

	// LockTimeout is how long to wait for lock acquisition
	LockTimeout = 60 * time.Second
)

// CNILock represents a system-wide lock for CNI operations
type CNILock struct {
	file *os.File
	path string
}

// AcquireCNILock acquires a system-wide lock for CNI operations
// This ensures controlled concurrency across all CNI processes
// TODO(debug): Remove detailed logging after debugging is complete
func AcquireCNILock() (*CNILock, error) {
	pid := os.Getpid()
	startTime := time.Now()

	// Ensure lock directory exists
	if err := os.MkdirAll(LockDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lock directory: %v", err)
	}

	// Open lock file
	f, err := os.OpenFile(SerialLockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %v", err)
	}

	klog.V(4).Infof("[CNI-DEBUG] Lock file opened: PID=%d, Path=%s, Time=%s",
		pid, SerialLockFile, time.Now().Format(time.RFC3339Nano))

	// Try to acquire lock with timeout
	deadline := time.Now().Add(LockTimeout)
	acquired := false
	retryCount := 0

	for time.Now().Before(deadline) && !acquired {
		// Try non-blocking lock
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			acquired = true
			break
		}

		// Check if error is "would block" (lock held by another process)
		if err != syscall.EWOULDBLOCK {
			f.Close()
			return nil, fmt.Errorf("failed to acquire lock: %v", err)
		}

		// Lock is busy, wait a bit and retry
		retryCount++
		if retryCount == 1 || retryCount%10 == 0 {
			// Log every 10th retry (every 500ms)
			waitTime := time.Since(startTime)
			klog.V(4).Infof("[CNI-DEBUG] Lock still held by another process: PID=%d, WaitTime=%v, Retries=%d",
				pid, waitTime, retryCount)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !acquired {
		f.Close()
		totalWaitTime := time.Since(startTime)
		klog.Warningf("[CNI-DEBUG] Lock acquisition timeout: PID=%d, WaitTime=%v, Retries=%d, Timeout=%v",
			pid, totalWaitTime, retryCount, LockTimeout)
		return nil, fmt.Errorf("timeout waiting for CNI lock after %v", LockTimeout)
	}

	acquireTime := time.Since(startTime)
	klog.Infof("[CNI-DEBUG] Lock acquisition successful: PID=%d, WaitTime=%v, Retries=%d, Time=%s",
		pid, acquireTime, retryCount, time.Now().Format(time.RFC3339Nano))

	return &CNILock{
		file: f,
		path: SerialLockFile,
	}, nil
}

// Release releases the CNI lock
// TODO(debug): Remove detailed logging after debugging is complete
func (l *CNILock) Release() error {
	if l.file == nil {
		return nil
	}

	pid := os.Getpid()
	klog.V(4).Infof("[CNI-DEBUG] Releasing lock: PID=%d, Path=%s, Time=%s",
		pid, l.path, time.Now().Format(time.RFC3339Nano))

	// Release lock
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		klog.Warningf("[CNI-DEBUG] Failed to unlock: PID=%d, Path=%s, Error=%v", pid, l.path, err)
	}

	// Close file
	if err := l.file.Close(); err != nil {
		klog.Warningf("[CNI-DEBUG] Failed to close lock file: PID=%d, Path=%s, Error=%v", pid, l.path, err)
		return err
	}

	klog.V(4).Infof("[CNI-DEBUG] Lock release completed: PID=%d, Path=%s, Time=%s",
		pid, l.path, time.Now().Format(time.RFC3339Nano))
	l.file = nil

	return nil
}
