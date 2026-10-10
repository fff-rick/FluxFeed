package applicationaccount

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	domainaccount "FluxFeed/internal/domain/account"
)

const (
	purposeVerifyEmail   = "verify_email"
	purposeResetPassword = "reset_password"
	purposeOAuthSession  = "oauth_session"
)

type Mailer interface {
	Send(ctx context.Context, to, subject, htmlBody string) error
}

type OAuthProfile struct {
	Subject, Email, Nickname, AvatarURL string
	EmailVerified                       bool
}

type OAuthProvider interface {
	Name() string
	AuthorizationURL(state, challenge string) string
	Exchange(ctx context.Context, code, verifier string) (*OAuthProfile, error)
}

type Option func(*Service)

func WithMailer(mailer Mailer, publicURL string) Option {
	return func(s *Service) { s.mailer, s.publicURL = mailer, strings.TrimRight(publicURL, "/") }
}

func WithPublicURL(publicURL string) Option {
	return func(s *Service) { s.publicURL = strings.TrimRight(publicURL, "/") }
}

func WithOAuthProvider(provider OAuthProvider) Option {
	return func(s *Service) {
		if provider != nil {
			s.providers[provider.Name()] = provider
		}
	}
}

type Capabilities struct {
	PasswordReset     bool     `json:"password_reset"`
	EmailVerification bool     `json:"email_verification"`
	OAuthProviders    []string `json:"oauth_providers"`
}

func (s *Service) Capabilities() Capabilities {
	providers := make([]string, 0, len(s.providers))
	for name := range s.providers {
		providers = append(providers, name)
	}
	sort.Strings(providers)
	return Capabilities{PasswordReset: s.mailer != nil, EmailVerification: s.mailer != nil, OAuthProviders: providers}
}

func (s *Service) OAuthProvider(name string) (OAuthProvider, bool) {
	provider, ok := s.providers[strings.ToLower(strings.TrimSpace(name))]
	return provider, ok
}

func (s *Service) PublicURL() string { return s.publicURL }

func (s *Service) SendEmailVerification(ctx context.Context, userID int64) error {
	if s.mailer == nil {
		return nil
	}
	user, err := s.repo.FindByID(ctx, userID)
	if err != nil {
		return err
	}
	if user.EmailVerified() {
		return nil
	}
	raw, err := s.issueActionToken(ctx, user.ID, purposeVerifyEmail, 24*time.Hour)
	if err != nil {
		return err
	}
	link := s.publicURL + "/auth#verify-email=" + raw
	body := fmt.Sprintf("<p>你好，%s：</p><p>请点击下面的链接验证 FluxFeed 邮箱（24 小时内有效）：</p><p><a href=\"%s\">验证邮箱</a></p>", html.EscapeString(user.Nickname), html.EscapeString(link))
	return s.mailer.Send(ctx, user.Account, "验证你的 FluxFeed 邮箱", body)
}

func (s *Service) VerifyEmail(ctx context.Context, raw string) error {
	userID, err := s.consumeActionToken(ctx, purposeVerifyEmail, raw)
	if err != nil {
		return err
	}
	return s.repo.MarkEmailVerified(ctx, userID, time.Now())
}

func (s *Service) RequestPasswordReset(ctx context.Context, account string) error {
	if s.mailer == nil {
		return nil
	}
	account, err := normalizeEmail(account)
	if err != nil {
		return nil
	}
	user, err := s.repo.FindByAccount(ctx, account)
	if err != nil || !user.EmailVerified() {
		return nil
	}
	raw, err := s.issueActionToken(ctx, user.ID, purposeResetPassword, 30*time.Minute)
	if err != nil {
		return err
	}
	link := s.publicURL + "/auth#reset-password=" + raw
	body := fmt.Sprintf("<p>你好，%s：</p><p>请点击下面的链接重置 FluxFeed 密码（30 分钟内有效）：</p><p><a href=\"%s\">重置密码</a></p>", html.EscapeString(user.Nickname), html.EscapeString(link))
	return s.mailer.Send(ctx, user.Account, "重置你的 FluxFeed 密码", body)
}

func (s *Service) ResetPassword(ctx context.Context, raw, password string) error {
	if len(strings.TrimSpace(password)) < 8 {
		return domainaccount.ErrWeakPassword
	}
	userID, err := s.consumeActionToken(ctx, purposeResetPassword, raw)
	if err != nil {
		return err
	}
	user, err := s.repo.FindByID(ctx, userID)
	if err != nil {
		return err
	}
	if err := user.SetPassword(password); err != nil {
		return err
	}
	return s.repo.UpdatePassword(ctx, user)
}

func (s *Service) CompleteOAuth(ctx context.Context, providerName string, profile *OAuthProfile) (string, error) {
	if profile == nil || strings.TrimSpace(profile.Subject) == "" || !profile.EmailVerified {
		return "", domainaccount.ErrInvalidCredentials
	}
	email, err := normalizeEmail(profile.Email)
	if err != nil {
		return "", domainaccount.ErrInvalidCredentials
	}
	user, err := s.repo.FindByExternalIdentity(ctx, providerName, profile.Subject)
	if errors.Is(err, domainaccount.ErrUserNotFound) {
		user, err = s.repo.FindByAccount(ctx, email)
		if errors.Is(err, domainaccount.ErrUserNotFound) {
			password, randomErr := randomToken(32)
			if randomErr != nil {
				return "", randomErr
			}
			nickname := strings.TrimSpace(profile.Nickname)
			if nickname == "" {
				nickname = strings.Split(email, "@")[0]
			}
			user, err = domainaccount.New(email, password, nickname)
			if err == nil {
				user.AvatarURL = strings.TrimSpace(profile.AvatarURL)
				err = s.repo.Save(ctx, user)
			}
		}
		if err != nil {
			return "", err
		}
		if err = s.repo.MarkEmailVerified(ctx, user.ID, time.Now()); err != nil {
			return "", err
		}
		user.EmailVerifiedAt = timePointer(time.Now())
		err = s.repo.LinkExternalIdentity(ctx, domainaccount.ExternalIdentity{UserID: user.ID, Provider: providerName, ProviderSubject: profile.Subject, Email: email})
		if errors.Is(err, domainaccount.ErrExternalIdentityExists) {
			user, err = s.repo.FindByExternalIdentity(ctx, providerName, profile.Subject)
		}
	}
	if err != nil {
		return "", err
	}
	return s.issueActionToken(ctx, user.ID, purposeOAuthSession, 2*time.Minute)
}

func (s *Service) ExchangeOAuthSession(ctx context.Context, raw string) (*LoginResult, error) {
	userID, err := s.consumeActionToken(ctx, purposeOAuthSession, raw)
	if err != nil {
		return nil, err
	}
	user, err := s.repo.FindByID(ctx, userID)
	if err != nil || user.Status != domainaccount.StatusNormal {
		return nil, domainaccount.ErrInvalidCredentials
	}
	accessToken, err := s.signer.SignAccessToken(user.ID, user.Role)
	if err != nil {
		return nil, ErrSignAccessTokenFailed
	}
	return &LoginResult{AccessToken: accessToken, TokenType: "Bearer", ExpiresInSeconds: int64(s.signer.AccessTTL().Seconds())}, nil
}

func (s *Service) issueActionToken(ctx context.Context, userID int64, purpose string, ttl time.Duration) (string, error) {
	raw, err := randomToken(32)
	if err != nil {
		return "", err
	}
	err = s.repo.SaveActionToken(ctx, domainaccount.ActionToken{UserID: userID, Purpose: purpose, TokenHash: hashToken(raw), ExpiresAt: time.Now().Add(ttl)})
	return raw, err
}

func (s *Service) consumeActionToken(ctx context.Context, purpose, raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, domainaccount.ErrInvalidActionToken
	}
	return s.repo.ConsumeActionToken(ctx, purpose, hashToken(raw), time.Now())
}

func randomToken(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func timePointer(value time.Time) *time.Time { return &value }
