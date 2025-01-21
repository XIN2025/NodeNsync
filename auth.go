package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type UserRole string

const (
	RoleAdmin  UserRole = "admin"
	RoleUser   UserRole = "user"
	RoleReader UserRole = "reader"
)

type User struct {
	Username     string
	PasswordHash string
	Role         UserRole
	CreatedAt    time.Time
}

type Session struct {
	Token      string
	UserID     string
	Role       UserRole
	ExpiresAt  time.Time
	LastActive time.Time
}

type AuthManager struct {
	mu       sync.RWMutex
	users    map[string]*User
	sessions map[string]*Session

	sessionDuration time.Duration
	cleanupInterval time.Duration
}

func NewAuthManager() *AuthManager {
	am := &AuthManager{
		users:           make(map[string]*User),
		sessions:        make(map[string]*Session),
		sessionDuration: 24 * time.Hour,
		cleanupInterval: 2 * time.Minute,
	}

	am.CreateUser("a", "a", RoleAdmin)

	go am.cleanupLoop()
	return am
}

func (am *AuthManager) CreateUser(username, password string, role UserRole) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.users[username]; exists {
		return fmt.Errorf("user already exists")
	}

	hashedPass := hashPassword(password)
	am.users[username] = &User{
		Username:     username,
		PasswordHash: hashedPass,
		Role:         role,
		CreatedAt:    time.Now(),
	}
	return nil
}
func (am *AuthManager) Authenticate(username, password string) (string, error) {
	am.mu.RLock()
	user, exists := am.users[username]
	am.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("invalid credentials")
	}

	if user.PasswordHash != hashPassword(password) {
		return "", fmt.Errorf("invalid credentials")
	}

	token := generateToken()
	session := &Session{
		Token:      token,
		UserID:     username,
		Role:       user.Role,
		ExpiresAt:  time.Now().Add(am.sessionDuration),
		LastActive: time.Now(),
	}

	am.mu.Lock()
	am.sessions[token] = session
	am.mu.Unlock()

	return token, nil
}

func (am *AuthManager) ValidateSession(token string) (*Session, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	session, exists := am.sessions[token]
	if !exists {
		return nil, fmt.Errorf("invalid session")
	}

	if time.Now().After(session.ExpiresAt) {
		return nil, fmt.Errorf("session expired")
	}

	return session, nil
}

func (am *AuthManager) cleanupLoop() {
	ticker := time.NewTicker(am.cleanupInterval)
	for range ticker.C {
		am.cleanupExpiredSessions()
	}
}

func (am *AuthManager) cleanupExpiredSessions() {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()
	for token, session := range am.sessions {
		if now.After(session.ExpiresAt) {
			delete(am.sessions, token)
		}
	}
}

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateToken() string {

	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(hash[:])
}
