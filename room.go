package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Room struct {
	Input string `json:"input"`
	ID    string `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner,omitempty"`
	Live  string `json:"live,omitempty"`
}

var roomInputPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
var roomIDPattern = regexp.MustCompile(`^[0-9]{1,20}$`)

func parseRoomInput(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", errors.New("请输入房间号、房间别名或斗鱼链接")
	}
	if strings.HasPrefix(input, "www.douyu.com/") || strings.HasPrefix(input, "douyu.com/") || strings.HasPrefix(input, "m.douyu.com/") {
		input = "https://" + input
	}
	if strings.Contains(input, "://") {
		u, err := url.Parse(input)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
			return "", errors.New("房间链接格式不正确")
		}
		switch strings.ToLower(u.Hostname()) {
		case "douyu.com", "www.douyu.com", "m.douyu.com":
		default:
			return "", errors.New("请使用 douyu.com 的房间链接")
		}
		input = strings.Trim(u.Path, "/")
		if rid := u.Query().Get("rid"); rid != "" {
			input = rid
		}
	}
	if !roomInputPattern.MatchString(input) {
		return "", errors.New("无法识别房间；请粘贴房间主页链接，或输入房间号 / 别名")
	}
	return input, nil
}

type roomResolver struct {
	HTTP       *http.Client
	APIBase    string
	PageBase   string
	MobileBase string
}

func defaultResolver() roomResolver {
	return roomResolver{HTTP: &http.Client{Timeout: 15 * time.Second}, APIBase: "https://open.douyucdn.cn/api/RoomApi/room/", PageBase: "https://www.douyu.com/", MobileBase: "https://m.douyu.com/"}
}

func (r roomResolver) get(ctx context.Context, target string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2*maxPacketSize+1))
	if len(data) > 2*maxPacketSize {
		return nil, errors.New("房间信息过大")
	}
	return data, err
}

func jsonText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	return string(raw)
}

func (r roomResolver) fromAPI(ctx context.Context, input string) (Room, error) {
	data, err := r.get(ctx, r.APIBase+url.PathEscape(input))
	if err != nil {
		return Room{}, err
	}
	var result struct {
		Error int `json:"error"`
		Data  struct {
			ID     json.RawMessage `json:"room_id"`
			Name   string          `json:"room_name"`
			Owner  string          `json:"owner_name"`
			Status json.RawMessage `json:"room_status"`
		} `json:"data"`
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return Room{}, err
	}
	id := jsonText(result.Data.ID)
	if result.Error != 0 || !roomIDPattern.MatchString(id) || id == "0" {
		return Room{}, fmt.Errorf("房间 API 未返回有效房间（error=%d）", result.Error)
	}
	return Room{Input: input, ID: id, Name: result.Data.Name, Owner: result.Data.Owner, Live: jsonText(result.Data.Status)}, nil
}

var mobileContextPattern = regexp.MustCompile(`(?is)<script\b[^>]*\bid\s*=\s*["']vike_pageContext["'][^>]*>(.*?)</script\s*>`)

func (r roomResolver) fromMobile(ctx context.Context, input string) (Room, error) {
	page, err := r.get(ctx, r.MobileBase+url.PathEscape(input))
	if err != nil {
		return Room{}, err
	}
	match := mobileContextPattern.FindSubmatch(page)
	if match == nil {
		return Room{}, errors.New("手机版网页中没有找到房间信息")
	}
	var data struct {
		PageProps struct {
			Room struct {
				RoomInfo struct {
					RoomInfo struct {
						ID    json.RawMessage `json:"rid"`
						Name  string          `json:"roomName"`
						Owner string          `json:"nickname"`
						Live  json.RawMessage `json:"isLive"`
					} `json:"roomInfo"`
				} `json:"roomInfo"`
			} `json:"room"`
		} `json:"pageProps"`
	}
	if err := json.Unmarshal(match[1], &data); err != nil {
		return Room{}, err
	}
	info := data.PageProps.Room.RoomInfo.RoomInfo
	id := jsonText(info.ID)
	if !roomIDPattern.MatchString(id) || id == "0" {
		return Room{}, errors.New("手机版网页未返回有效的真实房间号")
	}
	return Room{Input: input, ID: id, Name: info.Name, Owner: info.Owner, Live: jsonText(info.Live)}, nil
}

var pageRoomPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\$ROOM\.room_id\s*=\s*["']?([0-9]+)`),
	regexp.MustCompile(`(?i)<link[^>]+rel=["']canonical["'][^>]+href=["']https?://(?:www\.)?douyu\.com/([0-9]+)`),
	regexp.MustCompile(`(?:\\?["'])room_id(?:\\?["'])\s*:\s*(?:\\?["'])?([0-9]+)`),
}
var titlePattern = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)

func (r roomResolver) Resolve(ctx context.Context, raw string) (Room, error) {
	input, err := parseRoomInput(raw)
	if err != nil {
		return Room{}, err
	}
	// mobileTried 与 mobileErr 分开记：手机页成功会直接返回，所以往下走时 mobileErr
	// 为 nil 只可能意味着这条路压根没开。以前靠 err 是否为 nil 兼职表达这件事，
	// 三处判断都得反着读。
	mobileTried := r.MobileBase != ""
	var mobileErr error
	if mobileTried {
		// 短号在旧 API 里可能指到别的房间，手机页给的才是真 RID。
		mobileCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var room Room
		room, mobileErr = r.fromMobile(mobileCtx, input)
		cancel()
		if mobileErr == nil {
			return room, nil
		}
		if ctx.Err() != nil {
			return Room{}, ctx.Err()
		}
	}
	room, apiErr := r.fromAPI(ctx, input)
	// 手机页失败过就不能就此收工：短号在旧 API 里可能指到别的房间，还要拿桌面页核对。
	if apiErr == nil && !mobileTried {
		return room, nil
	}
	if ctx.Err() != nil {
		return Room{}, ctx.Err()
	}
	page, pageErr := r.get(ctx, r.PageBase+url.PathEscape(input))
	if pageErr == nil {
		for _, pattern := range pageRoomPatterns {
			if match := pattern.FindSubmatch(page); match != nil && string(match[1]) != "0" {
				name := "斗鱼直播间 " + string(match[1])
				if title := titlePattern.FindSubmatch(page); title != nil {
					name = strings.TrimSpace(html.UnescapeString(string(title[1])))
				}
				id := string(match[1])
				if apiErr == nil && room.ID == id {
					room.Name = first(room.Name, name)
					return room, nil
				}
				if mobileTried && id != input {
					// 只采用 RID 与桌面页一致的那份元数据。
					canonical, err := r.fromAPI(ctx, id)
					if err == nil && canonical.ID == id {
						canonical.Input = input
						canonical.Name = first(canonical.Name, name)
						return canonical, nil
					}
					if ctx.Err() != nil {
						return Room{}, ctx.Err()
					}
				}
				return Room{Input: input, ID: string(match[1]), Name: name}, nil
			}
		}
		pageErr = errors.New("网页中没有找到真实房间号")
	}
	if ctx.Err() != nil {
		return Room{}, ctx.Err()
	}
	if apiErr == nil {
		return room, nil
	}
	if mobileTried {
		return Room{}, fmt.Errorf("房间信息获取失败（手机版: %v；API: %v；网页: %v）", mobileErr, apiErr, pageErr)
	}
	return Room{}, fmt.Errorf("房间信息获取失败（API: %v；网页: %v）", apiErr, pageErr)
}
