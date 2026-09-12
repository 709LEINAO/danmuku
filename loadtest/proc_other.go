//go:build !linux

package main

import (
	"fmt"
	"os/exec"
	"runtime"
)

type hostInfo struct {
	OS   string `json:"os"`
	CPUs int    `json:"cpus"`
}

func (h hostInfo) Summary() string {
	return fmt.Sprintf("%s，%d 核（非 Linux，不采样进程 CPU／内存）", h.OS, h.CPUs)
}

func describeHost() hostInfo {
	return hostInfo{OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()}
}

func sampleProcess(int) (float64, int64, int, bool) { return 0, 0, 0, false }

func selfCPU() float64 { return -1 }

func terminate(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill()
	}
}
