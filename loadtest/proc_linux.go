//go:build linux

package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

const clockTicks = 100 // Linux 的 /proc 统一按 USER_HZ=100 报 CPU 时间

type hostInfo struct {
	OS        string `json:"os"`
	CPUs      int    `json:"cpus"`
	Model     string `json:"cpuModel,omitempty"`
	MemoryMiB int64  `json:"memoryMiB,omitempty"`
	Kernel    string `json:"kernel,omitempty"`
}

func (h hostInfo) Summary() string {
	return fmt.Sprintf("%s，%d 核 %s，内存 %d MiB，%s", h.OS, h.CPUs, h.Model, h.MemoryMiB, h.Kernel)
}

func describeHost() hostInfo {
	info := hostInfo{OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()}
	if file, err := os.Open("/proc/cpuinfo"); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if name, value, ok := strings.Cut(scanner.Text(), ":"); ok && strings.TrimSpace(name) == "model name" {
				info.Model = strings.TrimSpace(value)
				break
			}
		}
		file.Close()
	}
	if file, err := os.Open("/proc/meminfo"); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if name, value, ok := strings.Cut(scanner.Text(), ":"); ok && name == "MemTotal" {
				if kb, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "kB")), 10, 64); err == nil {
					info.MemoryMiB = kb / 1024
				}
				break
			}
		}
		file.Close()
	}
	if data, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		info.Kernel = strings.TrimSpace(string(data))
	}
	return info
}

// 进程累计 CPU 秒、RSS（kB）、线程数。
func sampleProcess(pid int) (float64, int64, int, bool) {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, 0, false
	}
	// comm 可能含空格，先跳到最后一个 ')'。之后 fields[0] 是 state（第 3 项），utime、stime 是第 14、15 项。
	text := string(stat)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return 0, 0, 0, false
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 13 {
		return 0, 0, 0, false
	}
	utime, err1 := strconv.ParseFloat(fields[11], 64)
	stime, err2 := strconv.ParseFloat(fields[12], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, 0, false
	}
	var rss int64
	var threads int
	if file, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status"); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			name, value, ok := strings.Cut(scanner.Text(), ":")
			if !ok {
				continue
			}
			value = strings.TrimSpace(value)
			switch name {
			case "VmRSS":
				rss, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(value, "kB")), 10, 64)
			case "Threads":
				threads, _ = strconv.Atoi(value)
			}
		}
		file.Close()
	}
	return (utime + stime) / clockTicks, rss, threads, true
}

// 本工具自己的累计 CPU 秒；拿不到返回 -1。
func selfCPU() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return -1
	}
	return float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6 + float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
}

// SIGTERM 让服务进程走正常关闭路径。
func terminate(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Signal(syscall.SIGTERM)
	}
}
