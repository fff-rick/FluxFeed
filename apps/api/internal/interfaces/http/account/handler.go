package interfaceshttpaccount

import (
	applicationaccount "FluxFeed/internal/application/account"
	domainaccount "FluxFeed/internal/domain/account"
	interfaceshttpmiddleware "FluxFeed/internal/interfaces/http/middleware"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *applicationaccount.Service
}

// New 注入账号应用服务，Handler 只处理 HTTP 输入输出。
func New(service *applicationaccount.Service) *Handler {
	return &Handler{
		service: service,
	}
}

// Register 处理用户注册请求，成功后返回新用户资料。
func (h *Handler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// 具体注册规则在应用层和领域层执行，HTTP 层只传递请求字段。
	profile, err := h.service.Register(c.Request.Context(), req.Account, req.Password, req.Nickname)
	if err != nil {
		if isBadRequestError(err) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if errors.Is(err, domainaccount.ErrAccountAlreadyExists) {
			c.JSON(http.StatusConflict, gin.H{"error": "account already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	c.JSON(http.StatusCreated, profileResponse(profile))
}

// Login 处理账号密码登录，成功后返回 Bearer token。
func (h *Handler) Login(c *gin.Context) {
	var req LoginByPasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// 登录失败统一映射为 401，避免暴露账号是否存在。
	token, err := h.service.Login(c.Request.Context(), req.Account, req.Password)
	if err != nil {
		if isBadRequestError(err) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if errors.Is(err, domainaccount.ErrInvalidCredentials) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	c.JSON(http.StatusOK, tokenResponse{
		AccessToken:      token.AccessToken,
		TokenType:        token.TokenType,
		ExpiresInSeconds: token.ExpiresInSeconds,
	})
}

// Logout 当前项目使用无状态 JWT，服务端无需清理会话数据。
func (h *Handler) Logout(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// Me 读取当前登录用户资料，用户 ID 来自 JWT 中间件写入的上下文。
func (h *Handler) Me(c *gin.Context) {
	userID, ok := userIDFromContext(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid access token"})
		return
	}

	profile, err := h.service.GetProfile(c.Request.Context(), userID)
	if err != nil {
		writeProfileError(c, err)
		return
	}

	c.JSON(http.StatusOK, profileResponse(profile))
}

// Get 读取公开用户资料，用于访问他人主页。
func (h *Handler) Get(c *gin.Context) {
	userID, err := parsePositiveUserID(c.Param("userId"))
	if err != nil {
		writeProfileError(c, err)
		return
	}

	profile, err := h.service.GetPublicProfile(c.Request.Context(), userID)
	if err != nil {
		writeProfileError(c, err)
		return
	}

	c.JSON(http.StatusOK, publicProfileResponse(profile))
}

// UpdateMe 更新当前登录用户资料，请求体支持部分字段更新。
func (h *Handler) UpdateMe(c *gin.Context) {
	userID, ok := userIDFromContext(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid access token"})
		return
	}

	var req UpdateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	profile, err := h.service.UpdateProfile(c.Request.Context(), userID, req.Nickname, req.AvatarURL, req.Bio)
	if err != nil {
		writeProfileError(c, err)
		return
	}

	c.JSON(http.StatusOK, profileResponse(profile))
}

// publicProfileResponse 将应用层 Profile 转成公开 JSON 结构。
func publicProfileResponse(profile *applicationaccount.Profile) publicUserProfileResponse {
	return publicUserProfileResponse{
		ID:             profile.ID,
		Nickname:       profile.Nickname,
		AvatarURL:      profile.AvatarURL,
		Bio:            profile.Bio,
		FollowingCount: profile.FollowingCount,
		FollowerCount:  profile.FollowerCount,
		WorkCount:      profile.WorkCount,
	}
}

// profileResponse 将应用层 Profile 转成对外 JSON 结构。
func profileResponse(profile *applicationaccount.Profile) userProfileResponse {
	return userProfileResponse{
		ID:                    profile.ID,
		Account:               profile.Account,
		Nickname:              profile.Nickname,
		AvatarURL:             profile.AvatarURL,
		Bio:                   profile.Bio,
		Status:                profile.Status,
		Role:                  profile.Role,
		FollowingCount:        profile.FollowingCount,
		FollowerCount:         profile.FollowerCount,
		WorkCount:             profile.WorkCount,
		EmailVerified:         profile.EmailVerified,
		VerificationEmailSent: profile.VerificationEmailSent,
	}
}

// userIDFromContext 从 JWT 中间件写入的上下文中读取登录用户 ID。
func userIDFromContext(c *gin.Context) (int64, bool) {
	value, exists := c.Get(interfaceshttpmiddleware.ContextUserIDKey)
	if !exists {
		return 0, false
	}
	userID, ok := value.(int64)
	return userID, ok && userID > 0
}

func parsePositiveUserID(raw string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value <= 0 {
		return 0, domainaccount.ErrInvalidUserID
	}
	return value, nil
}

// writeProfileError 统一账号资料相关接口的错误响应。
func writeProfileError(c *gin.Context, err error) {
	if isBadRequestError(err) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if errors.Is(err, domainaccount.ErrUserNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}

// isBadRequestError 判断哪些领域错误属于客户端请求参数问题。
func isBadRequestError(err error) bool {
	return errors.Is(err, domainaccount.ErrEmptyAccount) ||
		errors.Is(err, domainaccount.ErrInvalidEmail) ||
		errors.Is(err, domainaccount.ErrEmptyPassword) ||
		errors.Is(err, domainaccount.ErrWeakPassword) ||
		errors.Is(err, domainaccount.ErrEmptyNickname) ||
		errors.Is(err, domainaccount.ErrInvalidUserID) ||
		errors.Is(err, domainaccount.ErrEmptyProfileUpdate)
}

func (h *Handler) Capabilities(c *gin.Context) { c.JSON(http.StatusOK, h.service.Capabilities()) }

func (h *Handler) RequestEmailVerification(c *gin.Context) {
	userID, ok := userIDFromContext(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid access token"})
		return
	}
	if err := h.service.SendEmailVerification(c.Request.Context(), userID); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "verification email could not be sent"})
		return
	}
	c.Status(http.StatusAccepted)
}

func (h *Handler) VerifyEmail(c *gin.Context) {
	var request actionTokenRequest
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if err := h.service.VerifyEmail(c.Request.Context(), request.Token); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "verification link is invalid or expired"})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) RequestPasswordReset(c *gin.Context) {
	var request passwordResetRequest
	if c.ShouldBindJSON(&request) == nil {
		_ = h.service.RequestPasswordReset(c.Request.Context(), request.Account)
	}
	c.Status(http.StatusAccepted)
}

func (h *Handler) ResetPassword(c *gin.Context) {
	var request passwordResetConfirmRequest
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if err := h.service.ResetPassword(c.Request.Context(), request.Token, request.Password); err != nil {
		if errors.Is(err, domainaccount.ErrWeakPassword) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "reset link is invalid or expired"})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) BeginOAuth(c *gin.Context) {
	name := strings.ToLower(c.Param("provider"))
	provider, ok := h.service.OAuthProvider(name)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "oauth provider is not enabled"})
		return
	}
	state, err := randomOAuthValue()
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	verifier, err := randomOAuthValue()
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	challengeBytes := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes[:])
	secure := strings.HasPrefix(h.service.PublicURL(), "https://")
	path := "/api/oauth/" + name + "/callback"
	http.SetCookie(c.Writer, &http.Cookie{Name: "fluxfeed_oauth_state", Value: state, Path: path, MaxAge: 600, HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode})
	http.SetCookie(c.Writer, &http.Cookie{Name: "fluxfeed_oauth_verifier", Value: verifier, Path: path, MaxAge: 600, HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode})
	c.Redirect(http.StatusFound, provider.AuthorizationURL(state, challenge))
}

func (h *Handler) FinishOAuth(c *gin.Context) {
	name := strings.ToLower(c.Param("provider"))
	provider, ok := h.service.OAuthProvider(name)
	if !ok {
		h.oauthRedirect(c, "auth-error", "oauth provider is not enabled")
		return
	}
	stateCookie, stateErr := c.Request.Cookie("fluxfeed_oauth_state")
	verifierCookie, verifierErr := c.Request.Cookie("fluxfeed_oauth_verifier")
	if stateErr != nil || verifierErr != nil || subtle.ConstantTimeCompare([]byte(stateCookie.Value), []byte(c.Query("state"))) != 1 {
		h.oauthRedirect(c, "auth-error", "oauth state validation failed")
		return
	}
	profile, err := provider.Exchange(c.Request.Context(), c.Query("code"), verifierCookie.Value)
	if err != nil {
		h.oauthRedirect(c, "auth-error", "oauth authorization failed")
		return
	}
	code, err := h.service.CompleteOAuth(c.Request.Context(), name, profile)
	if err != nil {
		h.oauthRedirect(c, "auth-error", "oauth account could not be created")
		return
	}
	h.oauthRedirect(c, "oauth-code", code)
}

func (h *Handler) ExchangeOAuthSession(c *gin.Context) {
	var request oauthSessionRequest
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	token, err := h.service.ExchangeOAuthSession(c.Request.Context(), request.Code)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "oauth code is invalid or expired"})
		return
	}
	c.JSON(http.StatusOK, tokenResponse{AccessToken: token.AccessToken, TokenType: token.TokenType, ExpiresInSeconds: token.ExpiresInSeconds})
}

func (h *Handler) oauthRedirect(c *gin.Context, key, value string) {
	target := strings.TrimRight(h.service.PublicURL(), "/") + "/auth#" + key + "=" + url.QueryEscape(value)
	c.Redirect(http.StatusFound, target)
}

func randomOAuthValue() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}
