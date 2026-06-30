// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Adapted from github.com/rs/xid/hostid_fallback.go.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

//go:build !darwin && !linux && !freebsd && !windows

package machine

import "errors"

func readPlatformMachineID() (string, error) {
	return "", errors.New("machine: platform id not implemented")
}
