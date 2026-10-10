package infragovernance

import "testing"

func TestRuleProvider(t *testing.T) {
	provider := RuleProvider{}
	for _, test := range []struct{ content, want string }{{"ordinary title", VerdictPass}, {"[review] uncertain", VerdictReview}, {"包含违禁内容", VerdictReject}} {
		got, _, _ := provider.Review("title", test.content)
		if got != test.want {
			t.Fatalf("Review(%q)=%q want %q", test.content, got, test.want)
		}
	}
}

func TestApplyVideoHumanGate(t *testing.T) {
	for _, test := range []struct {
		name       string
		items      []ReviewItemModel
		reasonCode string
	}{
		{name: "all machine checks pass", items: []ReviewItemModel{{Verdict: VerdictPass}, {Verdict: VerdictPass}, {Verdict: VerdictPass}}},
		{name: "machine requests review", items: []ReviewItemModel{{Verdict: VerdictPass}, {Verdict: VerdictReview, ReasonCode: "RULE_MANUAL", Reason: "人工复审"}}, reasonCode: "RULE_MANUAL"},
		{name: "machine rejects", items: []ReviewItemModel{{Verdict: VerdictReview, ReasonCode: "RULE_MANUAL"}, {Verdict: VerdictReject, ReasonCode: "RULE_REJECT", Reason: "违禁"}}, reasonCode: "RULE_REJECT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			review := ReviewModel{TargetType: TargetVideo, Status: ReviewPending}
			if !applyVideoHumanGate(&review, test.items) {
				t.Fatal("expected video review to require a human decision")
			}
			if review.Status != ReviewHuman || review.ReasonCode != test.reasonCode {
				t.Fatalf("unexpected review result: %+v", review)
			}
		})
	}
}

func TestApplyVideoHumanGateDoesNotChangeComments(t *testing.T) {
	review := ReviewModel{TargetType: TargetComment, Status: ReviewPending}
	if applyVideoHumanGate(&review, []ReviewItemModel{{Verdict: VerdictPass}}) {
		t.Fatal("comment review must keep its existing automatic flow")
	}
	if review.Status != ReviewPending {
		t.Fatalf("unexpected comment status: %s", review.Status)
	}
}

func TestOnlyHumanReviewCanBeDecided(t *testing.T) {
	if !canDecideReview(ReviewHuman) {
		t.Fatal("human_review must accept a reviewer decision")
	}
	for _, status := range []string{ReviewPending, ReviewPublished, ReviewRejected, ReviewCanceled} {
		if canDecideReview(status) {
			t.Fatalf("status %q must not accept a reviewer decision", status)
		}
	}
}
