package domainaccount

import "errors"

// 账号领域错误集中放在领域层，HTTP 层会把它们映射为对应状态码。
var ErrEmptyAccount = errors.New("account is required")
var ErrInvalidEmail = errors.New("account must be a valid email address")
var ErrEmptyPassword = errors.New("password is required")
var ErrWeakPassword = errors.New("password must contain at least 8 characters")
var ErrEmptyNickname = errors.New("nickname is required")
var ErrInvalidUserID = errors.New("user id must be positive")
var ErrEmptyProfileUpdate = errors.New("profile update is required")
var ErrUserNotFound = errors.New("user not found")
var ErrAccountAlreadyExists = errors.New("account already exists")
var ErrInvalidCredentials = errors.New("invalid credentials")
var ErrHashPasswordFailed = errors.New("hash password failed")
var ErrInvalidActionToken = errors.New("action token is invalid or expired")
var ErrExternalIdentityExists = errors.New("external identity already exists")
