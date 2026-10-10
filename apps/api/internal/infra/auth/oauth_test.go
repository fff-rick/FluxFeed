package infraauth

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestOAuthProviderExchangeProfiles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/token":
			if err := r.ParseForm(); err != nil || r.Form.Get("code_verifier") != "verifier" {
				t.Errorf("missing PKCE verifier: %v", r.Form)
			}
			fmt.Fprint(w, `{"access_token":"token","token_type":"Bearer"}`)
		case "/google":
			fmt.Fprint(w, `{"sub":"google-1","email":"user@example.com","name":"Google User","picture":"avatar","email_verified":true}`)
		case "/github":
			fmt.Fprint(w, `{"id":42,"login":"octocat","name":"GitHub User","avatar_url":"avatar"}`)
		case "/emails":
			fmt.Fprint(w, `[{"email":"user@example.com","primary":true,"verified":true}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	endpoint := oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}
	providers := []*OAuthProvider{
		{name: "google", config: oauth2.Config{ClientID: "id", ClientSecret: "secret", Endpoint: endpoint}, userURL: server.URL + "/google"},
		{name: "github", config: oauth2.Config{ClientID: "id", ClientSecret: "secret", Endpoint: endpoint}, userURL: server.URL + "/github", emailsURL: server.URL + "/emails"},
	}
	for _, provider := range providers {
		profile, err := provider.Exchange(t.Context(), "code", "verifier")
		if err != nil {
			t.Fatalf("%s exchange: %v", provider.Name(), err)
		}
		if profile.Email != "user@example.com" || !profile.EmailVerified || strings.TrimSpace(profile.Subject) == "" {
			t.Fatalf("unexpected %s profile: %+v", provider.Name(), profile)
		}
		url := provider.AuthorizationURL("state", "challenge")
		if !strings.Contains(url, "code_challenge=challenge") || !strings.Contains(url, "state=state") {
			t.Fatalf("missing state or PKCE in %s", url)
		}
	}
}
