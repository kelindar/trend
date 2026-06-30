// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Adapted from github.com/rs/xid/hostid_freebsd.go.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

//go:build freebsd

package machine

import "syscall"

func readPlatformMachineID() (string, error) {
	return syscall.Sysctl("kern.hostuuid")
}
