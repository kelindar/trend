// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Adapted from github.com/rs/xid/hostid_windows.go.
// Copyright (c) 2015 Olivier Poitrey. Used under the MIT license.

//go:build windows

package machine

import (
	"fmt"
	"syscall"
	"unsafe"
)

func readPlatformMachineID() (string, error) {
	var h syscall.Handle

	regKeyCryptoPtr, err := syscall.UTF16PtrFromString(`SOFTWARE\Microsoft\Cryptography`)
	if err != nil {
		return "", fmt.Errorf(`machine: read registry key "SOFTWARE\Microsoft\Cryptography": %w`, err)
	}

	err = syscall.RegOpenKeyEx(syscall.HKEY_LOCAL_MACHINE, regKeyCryptoPtr, 0, syscall.KEY_READ|syscall.KEY_WOW64_64KEY, &h)
	if err != nil {
		return "", err
	}
	defer func() { _ = syscall.RegCloseKey(h) }()

	const regBufLen = 74
	const uuidLen = 36

	var regBuf [regBufLen]uint16
	bufLen := uint32(regBufLen)
	var valType uint32

	mGuidPtr, err := syscall.UTF16PtrFromString(`MachineGuid`)
	if err != nil {
		return "", fmt.Errorf("machine: read machine GUID: %w", err)
	}

	err = syscall.RegQueryValueEx(h, mGuidPtr, nil, &valType, (*byte)(unsafe.Pointer(&regBuf[0])), &bufLen)
	if err != nil {
		return "", err
	}

	hostID := syscall.UTF16ToString(regBuf[:])
	if len(hostID) != uuidLen {
		return "", fmt.Errorf("machine: invalid host id %q", hostID)
	}
	return hostID, nil
}
