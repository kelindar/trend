// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Portions adapted from github.com/rs/xid.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

// Package machine identifies the current process for replica tags.
package machine

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

var (
	machineOnce sync.Once
	machine     uint32
	pid         = os.Getpid()
)

func init() {
	// If /proc/self/cpuset exists and is not /, assume a container and xor the
	// PID with the cpuset checksum for a more unique per-container value.
	b, err := os.ReadFile("/proc/self/cpuset")
	if err == nil && len(b) > 1 {
		pid ^= int(crc32.ChecksumIEEE(b))
	}
}

// ID returns a process replica ID packed as machine bits followed by PID bits.
func ID() int64 {
	return pack(machineID(), pid)
}

func machineID() uint32 {
	machineOnce.Do(func() {
		machine = readMachineID()
	})
	return machine
}

// readMachineID derives a host identifier from a platform-specific machine ID,
// hostname, or random bytes.
func readMachineID() uint32 {
	var id [4]byte
	hid, err := readPlatformMachineID()
	if err != nil || len(hid) == 0 {
		hid, err = os.Hostname()
	}
	if err == nil && len(hid) != 0 {
		hw := sha256.New()
		hw.Write([]byte(hid))
		copy(id[:], hw.Sum(nil))
	} else if _, err := rand.Read(id[:]); err != nil {
		return uint32(os.Getpid()) & 0x7fffffff
	}
	return binary.BigEndian.Uint32(id[:]) & 0x7fffffff
}

func pack(machine uint32, pid int) int64 {
	return int64(uint64(machine&0x7fffffff)<<32 | uint64(uint32(pid)))
}
