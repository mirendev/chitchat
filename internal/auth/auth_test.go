package auth

import "testing"

func TestAuthorize(t *testing.T) {
	const orgPrefix = "org:org-Miren-P20syIg0MAcS:"
	svcID := orgPrefix + "app:clusteragent:sandbox:sandbox/clusteragent-web-Cbpjuizsd85hsSukUjWHQ"

	cases := []struct {
		name     string
		domain   string
		prefixes []string
		email    string
		subject  string
		wantOK   bool
	}{
		{"no restrictions allows anything", "", nil, "anyone@anywhere", "sub", true},
		{"domain match", "miren.dev", nil, "alice@miren.dev", "s", true},
		{"domain mismatch", "miren.dev", nil, "bob@evil.com", "s", false},
		{"service id rejected with domain only", "miren.dev", nil, svcID, svcID, false},
		{"service id allowed by prefix", "miren.dev", []string{orgPrefix}, svcID, svcID, true},
		{"prefix matches subject when email empty", "", []string{orgPrefix}, "", svcID, true},
		{"other org service id rejected", "miren.dev", []string{orgPrefix}, "org:org-Other:app:x", "org:org-Other:app:x", false},
		{"human still restricted when prefixes set", "miren.dev", []string{orgPrefix}, "bob@evil.com", "s", false},
		{"human in domain allowed when prefixes set", "miren.dev", []string{orgPrefix}, "alice@miren.dev", "s", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := &Verifier{allowedDomain: tc.domain, allowedPrefixes: tc.prefixes}
			err := v.authorize(tc.email, tc.subject)
			if tc.wantOK && err != nil {
				t.Errorf("authorize(%q,%q) = %v, want allowed", tc.email, tc.subject, err)
			}
			if !tc.wantOK && err == nil {
				t.Errorf("authorize(%q,%q) = allowed, want rejected", tc.email, tc.subject)
			}
		})
	}
}
