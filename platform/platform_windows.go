//go:build windows
// +build windows

package platform

import (
	"os"

	"golang.org/x/sys/windows"
)

func EnableVirtualTerminal() {
	var flags uint32 = 0

	var handler = windows.Handle(os.Stdout.Fd())

	windows.GetConsoleMode(handler, &flags)
	windows.SetConsoleMode(handler, flags|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
