// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Adapted from github.com/rs/xid/hostid_darwin.go.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

//go:build darwin

package machine

import (
	"errors"
	"os/exec"
	"strings"
)

func readPlatformMachineID() (string, error) {
	ioreg, err := exec.LookPath("ioreg")
	if err != nil {
		return "", err
	}

	out, err := exec.Command(ioreg, "-rd1", "-c", "IOPlatformExpertDevice").CombinedOutput()
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "IOPlatformUUID") {
			parts := strings.SplitAfter(line, `" = "`)
			if len(parts) == 2 {
				uuid := strings.TrimRight(parts[1], `"`)
				return strings.ToLower(uuid), nil
			}
		}
	}

	return "", errors.New("machine: platform uuid not found")
}
