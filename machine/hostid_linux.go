// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Adapted from github.com/rs/xid/hostid_linux.go.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

//go:build linux

package machine

import "os"

func readPlatformMachineID() (string, error) {
	b, err := os.ReadFile("/etc/machine-id")
	if err != nil || len(b) == 0 {
		b, err = os.ReadFile("/sys/class/dmi/id/product_uuid")
	}
	return string(b), err
}
