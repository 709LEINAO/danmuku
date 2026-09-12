package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

const defaultRoom = "231059"
const settingsFilename = "douyu-danmaku.settings.json"

type serverSettings struct {
	DefaultRoom string `json:"defaultRoom"`
}

type settingsStore struct {
	mu    sync.RWMutex
	path  string
	value serverSettings
}

func loadSettings(path string) (*settingsStore, error) {
	store := &settingsStore{path: path, value: serverSettings{DefaultRoom: defaultRoom}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取设置文件 %s：%w", path, err)
	}
	var value serverSettings
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("设置文件 %s 格式不正确：%w", path, err)
	}
	room, err := parseRoomInput(value.DefaultRoom)
	if err != nil {
		return nil, fmt.Errorf("设置文件 %s 的默认房间无效：%w", path, err)
	}
	store.value = serverSettings{DefaultRoom: room}
	return store, nil
}

func (s *settingsStore) snapshot() serverSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value
}

// room 必须已过 parseRoomInput。
func (s *settingsStore) save(room string) (serverSettings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := serverSettings{DefaultRoom: room}
	file, err := os.CreateTemp(filepath.Dir(s.path), ".douyu-danmaku.settings-*.tmp")
	if err != nil {
		return s.value, err
	}
	defer os.Remove(file.Name())
	if err := json.NewEncoder(file).Encode(value); err != nil {
		file.Close()
		return s.value, err
	}
	if err := file.Close(); err != nil {
		return s.value, err
	}
	if err := os.Rename(file.Name(), s.path); err != nil {
		return s.value, err
	}
	s.value = value
	return value, nil
}

func (s *settingsStore) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.snapshot())
	})
	mux.HandleFunc("POST /api/settings", func(w http.ResponseWriter, r *http.Request) {
		var request serverSettings
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
		if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "设置数据格式不正确，请提供 defaultRoom 字符串"})
			return
		}
		room, err := parseRoomInput(request.DefaultRoom)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		value, err := s.save(room)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("无法保存默认房间设置：%v", err)})
			return
		}
		writeJSON(w, http.StatusOK, value)
	})
}
