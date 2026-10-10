package infraauth

import (
	applicationaccount "FluxFeed/internal/application/account"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/oauth2"
)

type OAuthProvider struct {
	name      string
	config    oauth2.Config
	userURL   string
	emailsURL string
}

func NewGoogleProvider(clientID, secret, publicURL string) *OAuthProvider {
	return &OAuthProvider{name: "google", config: oauth2.Config{ClientID: clientID, ClientSecret: secret, RedirectURL: strings.TrimRight(publicURL, "/") + "/api/oauth/google/callback", Scopes: []string{"openid", "email", "profile"}, Endpoint: oauth2.Endpoint{AuthURL: "https://accounts.google.com/o/oauth2/v2/auth", TokenURL: "https://oauth2.googleapis.com/token"}}, userURL: "https://openidconnect.googleapis.com/v1/userinfo"}
}

func NewGitHubProvider(clientID, secret, publicURL string) *OAuthProvider {
	return &OAuthProvider{name: "github", config: oauth2.Config{ClientID: clientID, ClientSecret: secret, RedirectURL: strings.TrimRight(publicURL, "/") + "/api/oauth/github/callback", Scopes: []string{"read:user", "user:email"}, Endpoint: oauth2.Endpoint{AuthURL: "https://github.com/login/oauth/authorize", TokenURL: "https://github.com/login/oauth/access_token"}}, userURL: "https://api.github.com/user", emailsURL: "https://api.github.com/user/emails"}
}

func (p *OAuthProvider) Name() string { return p.name }

func (p *OAuthProvider) AuthorizationURL(state, challenge string) string {
	return p.config.AuthCodeURL(state, oauth2.SetAuthURLParam("code_challenge", challenge), oauth2.SetAuthURLParam("code_challenge_method", "S256"))
}

func (p *OAuthProvider) Exchange(ctx context.Context, code, verifier string) (*applicationaccount.OAuthProfile, error) {
	token, err := p.config.Exchange(ctx, code, oauth2.SetAuthURLParam("code_verifier", verifier))
	if err != nil {
		return nil, err
	}
	client := p.config.Client(ctx, token)
	if p.name == "google" {
		return p.googleProfile(client)
	}
	return p.githubProfile(client)
}

func (p *OAuthProvider) googleProfile(client *http.Client) (*applicationaccount.OAuthProfile, error) {
	var response struct {
		Subject       string `json:"sub"`
		Email         string `json:"email"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		EmailVerified bool   `json:"email_verified"`
	}
	if err := getJSON(client, p.userURL, &response); err != nil {
		return nil, err
	}
	return &applicationaccount.OAuthProfile{Subject: response.Subject, Email: response.Email, Nickname: response.Name, AvatarURL: response.Picture, EmailVerified: response.EmailVerified}, nil
}

func (p *OAuthProvider) githubProfile(client *http.Client) (*applicationaccount.OAuthProfile, error) {
	var user struct {
		ID        int64  `json:"id"`
		Login     string `json:"login"`
		Name      string `json:"name"`
		AvatarURL string `json:"avatar_url"`
	}
	if err := getJSON(client, p.userURL, &user); err != nil {
		return nil, err
	}
	var emails []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}
	if err := getJSON(client, p.emailsURL, &emails); err != nil {
		return nil, err
	}
	email := ""
	for _, candidate := range emails {
		if candidate.Verified && (candidate.Primary || email == "") {
			email = candidate.Email
			if candidate.Primary {
				break
			}
		}
	}
	if email == "" {
		return nil, errors.New("github account has no verified email")
	}
	name := strings.TrimSpace(user.Name)
	if name == "" {
		name = user.Login
	}
	return &applicationaccount.OAuthProfile{Subject: strconv.FormatInt(user.ID, 10), Email: email, Nickname: name, AvatarURL: user.AvatarURL, EmailVerified: true}, nil
}

func getJSON(client *http.Client, endpoint string, target any) error {
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "FluxFeed")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("oauth profile request failed: %s", response.Status)
	}
	return json.NewDecoder(response.Body).Decode(target)
}

func OAuthProviderEnabled(clientID, secret string) bool {
	return strings.TrimSpace(clientID) != "" && strings.TrimSpace(secret) != ""
}
