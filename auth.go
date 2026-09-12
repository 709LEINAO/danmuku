package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	sessionCookieName   = "danmaku_session"
	sessionLifetime     = 24 * time.Hour
	loginFailureWindow  = time.Minute
	maxLoginFailures    = 5
	maxLoginPeers       = 1024
	maxAuthSessions     = 1024
	loginFailureMessage = "密码不正确或尝试过于频繁，请稍后重试。"
)

// 构建时经 -ldflags "-X main.defaultPassword=…" 注入；源码不含口令。
var defaultPassword string

//go:embed web/login.html
var loginHTML string

var loginPage = template.Must(template.New("login").Parse(loginHTML))

type authSession struct {
	expires time.Time
	done    chan struct{}
}

type loginFailures struct {
	count int
	until time.Time
}

type passwordAuth struct {
	mu           sync.Mutex
	passwordHash [32]byte
	sessions     map[[32]byte]*authSession
	failures     map[string]loginFailures
	now          func() time.Time
	lifetime     time.Duration
}

type authSessionKey struct{}

// 运行时 DANMAKU_PASSWORD 优先，其次构建时注入的值；两者皆空拒绝启动。
func configuredPassword() (string, error) {
	if value := os.Getenv("DANMAKU_PASSWORD"); value != "" {
		return value, nil
	}
	if defaultPassword != "" {
		return defaultPassword, nil
	}
	return "", errors.New("未设置访问密码：构建时以 DANMAKU_PASSWORD 注入，或运行时设置该环境变量")
}

func newPasswordAuth(password string) *passwordAuth {
	return &passwordAuth{
		passwordHash: sha256.Sum256([]byte(password)),
		sessions:     make(map[[32]byte]*authSession),
		failures:     make(map[string]loginFailures),
		now:          time.Now,
		lifetime:     sessionLifetime,
	}
}

// 限流只认 socket 对端，不信任转发头。
func loginPeer(r *http.Request) string {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return peer
}

func (a *passwordAuth) checkPassword(peer, password string) (bool, time.Duration) {
	provided := sha256.Sum256([]byte(password))
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for key, failure := range a.failures {
		if !now.Before(failure.until) {
			delete(a.failures, key)
		}
	}
	failure := a.failures[peer]
	if failure.count >= maxLoginFailures {
		return false, failure.until.Sub(now)
	}
	if subtle.ConstantTimeCompare(provided[:], a.passwordHash[:]) == 1 {
		delete(a.failures, peer)
		return true, 0
	}
	if failure.count == 0 {
		if len(a.failures) >= maxLoginPeers {
			return false, loginFailureWindow
		}
		failure.until = now.Add(loginFailureWindow)
	}
	failure.count++
	a.failures[peer] = failure
	return false, 0
}

func sessionToken(r *http.Request) string {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || len(cookie.Value) != 43 {
		return ""
	}
	return cookie.Value
}

func (a *passwordAuth) session(r *http.Request) *authSession {
	token := sessionToken(r)
	if token == "" {
		return nil
	}
	key := sha256.Sum256([]byte(token))
	a.mu.Lock()
	defer a.mu.Unlock()
	session := a.sessions[key]
	if session != nil && !a.now().Before(session.expires) {
		close(session.done)
		delete(a.sessions, key)
		return nil
	}
	return session
}

func (a *passwordAuth) revoke(r *http.Request) {
	key := sha256.Sum256([]byte(sessionToken(r)))
	a.mu.Lock()
	defer a.mu.Unlock()
	if session := a.sessions[key]; session != nil {
		close(session.done)
		delete(a.sessions, key)
	}
}

func (a *passwordAuth) issue(w http.ResponseWriter, r *http.Request) error {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	token := base64.RawURLEncoding.EncodeToString(random[:])
	key := sha256.Sum256([]byte(token))
	now := a.now()
	session := &authSession{expires: now.Add(a.lifetime), done: make(chan struct{})}
	a.revoke(r)
	a.mu.Lock()
	var oldestKey [32]byte
	var oldest *authSession
	for candidate, existing := range a.sessions {
		if !now.Before(existing.expires) {
			close(existing.done)
			delete(a.sessions, candidate)
		} else if oldest == nil || existing.expires.Before(oldest.expires) {
			oldestKey, oldest = candidate, existing
		}
	}
	if len(a.sessions) >= maxAuthSessions {
		close(oldest.done)
		delete(a.sessions, oldestKey)
	}
	a.sessions[key] = session
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Value: token, Path: "/",
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil,
		Expires: session.expires.UTC(), MaxAge: int(a.lifetime.Seconds()),
	})
	return nil
}

func clearSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Path: "/", MaxAge: -1, Expires: time.Unix(1, 0).UTC(),
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil,
	})
}

func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	value := r.Header.Get("Origin")
	if value == "" {
		value = r.Header.Get("Referer")
	}
	origin, err := url.Parse(value)
	return err == nil && origin.Scheme == scheme && origin.User == nil && strings.EqualFold(origin.Host, r.Host)
}

func safeLoginNext(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.User != nil ||
		!strings.HasPrefix(parsed.Path, "/") || strings.HasPrefix(parsed.Path, "//") ||
		strings.ContainsAny(value+parsed.Path, "\\\r\n\t") || parsed.Path == "/login" || parsed.Path == "/logout" {
		return "/"
	}
	return parsed.RequestURI()
}

func authAPIRequest(r *http.Request) bool {
	return r.URL.Path == "/events" || r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead)
}

// 对外只暴露这一个 handler，其中只有 /login 是公开的。
func (a *passwordAuth) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/login" {
			a.login(w, r)
			return
		}
		session := a.session(r)
		if session == nil {
			if authAPIRequest(r) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "请先登录"})
			} else {
				http.Redirect(w, r, "/login?next="+url.QueryEscape(safeLoginNext(r.URL.RequestURI())), http.StatusSeeOther)
			}
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions && !sameOrigin(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "请求来源无效"})
			return
		}
		if r.URL.Path == "/logout" && r.Method == http.MethodPost {
			a.revoke(r)
			clearSessionCookie(w, r)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authSessionKey{}, session)))
	})
}

func (a *passwordAuth) showLogin(w http.ResponseWriter, code int, next, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	loginPage.Execute(w, struct{ Next, Error string }{safeLoginNext(next), message})
}

func (a *passwordAuth) login(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		next := safeLoginNext(r.URL.Query().Get("next"))
		if a.session(r) != nil {
			http.Redirect(w, r, next, http.StatusSeeOther)
			return
		}
		message := ""
		if r.URL.Query().Get("expired") == "1" {
			message = "登录已过期，请重新输入密码。"
		}
		a.showLogin(w, http.StatusOK, next, message)
	case http.MethodPost:
		if !sameOrigin(r) {
			a.showLogin(w, http.StatusForbidden, "/", "请求来源无效，请重新打开登录页。")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := r.ParseForm(); err != nil {
			a.showLogin(w, http.StatusBadRequest, "/", loginFailureMessage)
			return
		}
		next := safeLoginNext(r.PostForm.Get("next"))
		ok, retry := a.checkPassword(loginPeer(r), r.PostForm.Get("password"))
		if !ok {
			code := http.StatusUnauthorized
			if retry > 0 {
				code = http.StatusTooManyRequests
				w.Header().Set("Retry-After", strconv.Itoa(int((retry+time.Second-1)/time.Second)))
			}
			a.showLogin(w, code, next, loginFailureMessage)
			return
		}
		if err := a.issue(w, r); err != nil {
			a.showLogin(w, http.StatusServiceUnavailable, next, "暂时无法登录，请稍后重试。")
			return
		}
		http.Redirect(w, r, next, http.StatusSeeOther)
	default:
		w.Header().Set("Allow", "GET, HEAD, POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}
