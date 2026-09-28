package infraaccount

import (
	"strings"
	"testing"
)

func TestAccountQueriesAvoidGlobalAggregateTables(t *testing.T) {
	for name, query := range map[string]string{
		"credentials": accountCredentialSelect(),
		"profile":     userWithStatSelect(),
	} {
		if strings.Contains(strings.ToUpper(query), "GROUP BY") {
			t.Fatalf("%s query must not aggregate complete tables: %s", name, query)
		}
	}
	profile := userWithStatSelect()
	for _, indexedFilter := range []string{
		"active_following.user_id = a.id",
		"active_follower.target_user_id = a.id",
		"published_work.author_id = a.id",
	} {
		if !strings.Contains(profile, indexedFilter) {
			t.Fatalf("profile query must use indexed per-user counts (%s): %s", indexedFilter, profile)
		}
	}
}
