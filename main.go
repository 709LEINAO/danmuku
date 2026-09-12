package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type startupOptions struct {
	Address    string
	Room       string
	RoomLimits roomManagerConfig
}

func parseStartupOptions(args []string, savedRoom string) (startupOptions, error) {
	options := startupOptions{RoomLimits: defaultRoomManagerConfig()}
	flags := flag.NewFlagSet("douyu-danmaku", flag.ContinueOnError)
	flags.StringVar(&options.Address, "addr", "0.0.0.0:8787", "网页监听地址，默认允许同局域网访问")
	flags.StringVar(&options.Room, "room", savedRoom, "兼容旧配置的房间参数；启动不自动连接，请在网页房间列表中打开房间")
	flags.IntVar(&options.RoomLimits.MaxRooms, "max-rooms", options.RoomLimits.MaxRooms, "同时保留的房间连接上限")
	flags.IntVar(&options.RoomLimits.MaxViewers, "max-viewers", options.RoomLimits.MaxViewers, "同时观看及等待解析的网页连接上限")
	flags.DurationVar(&options.RoomLimits.IdleGrace, "room-idle-grace", options.RoomLimits.IdleGrace, "最后一个观看者离开后的保留时长；0 表示立即回收")
	flags.IntVar(&options.RoomLimits.MaxDials, "max-dials", options.RoomLimits.MaxDials, "同时进行新建或重连握手的上限")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	if options.RoomLimits.MaxRooms < 1 || options.RoomLimits.MaxViewers < 1 || options.RoomLimits.MaxDials < 1 || options.RoomLimits.IdleGrace < 0 {
		return options, fmt.Errorf("连接上限必须大于 0，房间保留时长不能为负数")
	}
	return options, nil
}

func main() {
	executable, err := os.Executable()
	if err != nil {
		log.Fatalf("无法定位程序的设置文件：%v", err)
	}
	settings, err := loadSettings(filepath.Join(filepath.Dir(executable), settingsFilename))
	if err != nil {
		log.Fatal(err)
	}
	options, err := parseStartupOptions(os.Args[1:], settings.snapshot().DefaultRoom)
	if err != nil {
		if err == flag.ErrHelp {
			return
		}
		os.Exit(2)
	}
	password, err := configuredPassword()
	if err != nil {
		log.Fatal(err)
	}
	config := defaultClientConfig()
	if endpoints, err := upstreamOverride(os.Getenv("DANMAKU_UPSTREAM")); err != nil {
		log.Fatal(err)
	} else if endpoints != nil {
		config.Endpoints = endpoints
		log.Printf("弹幕上游已改为 %s（压测钩子 DANMAKU_UPSTREAM）", strings.Join(endpoints, ", "))
	}
	app := newApplication(defaultResolver(), config, options.RoomLimits, password)
	app.settings = settings
	app.icons.Start()
	listener, err := net.Listen("tcp", options.Address)
	if err != nil {
		log.Fatalf("无法启动本地网页：%v", err)
	}
	app.access = browserAddresses(listener.Addr())
	server := &http.Server{Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	log.Printf("斗鱼弹幕台已启动：%s （按 Ctrl+C 退出）", app.access.Local)
	for _, address := range app.access.LAN {
		log.Printf("局域网手机访问：%s", address)
	}
	log.Print("启动后仅监听网页；打开房间时才连接弹幕服务")
	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		if err != http.ErrServerClosed {
			log.Printf("本地网页服务错误：%v", err)
		}
	}
	app.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		server.Close()
	}
}

// 压测钩子：把弹幕上游换成自己的假服务器（loadtest 用），逗号分隔的 ws:// 或 wss:// 地址。
// 只影响弹幕连接；房间解析、礼物目录、徽章图仍走斗鱼。
func upstreamOverride(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var endpoints []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parsed, err := url.Parse(item)
		if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
			return nil, fmt.Errorf("DANMAKU_UPSTREAM 含无效地址 %q：只接受 ws:// 或 wss://", item)
		}
		endpoints = append(endpoints, item)
	}
	if len(endpoints) == 0 {
		return nil, errors.New("DANMAKU_UPSTREAM 已设置但没有有效地址")
	}
	return endpoints, nil
}

// 只描述本监听器的地址；远程浏览器用自己的 origin。
type accessInfo struct {
	Local string   `json:"local"`
	LAN   []string `json:"lan"`
}

func browserAddresses(address net.Addr) accessInfo {
	tcp := address.(*net.TCPAddr)
	port := strconv.Itoa(tcp.Port)
	urlFor := func(host string) string { return fmt.Sprintf("http://%s/", net.JoinHostPort(host, port)) }
	info := accessInfo{Local: urlFor("127.0.0.1"), LAN: []string{}}
	if !tcp.IP.IsUnspecified() {
		info.Local = urlFor(tcp.IP.String())
		if !tcp.IP.IsLoopback() {
			info.LAN = append(info.LAN, info.Local)
		}
		return info
	}
	interfaces, _ := net.Interfaces()
	seen := make(map[string]bool)
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, _ := iface.Addrs()
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil || ip.To4() == nil || !ip.IsPrivate() {
				continue
			}
			url := urlFor(ip.String())
			if !seen[url] {
				info.LAN = append(info.LAN, url)
				seen[url] = true
			}
		}
	}
	sort.Strings(info.LAN)
	return info
}
