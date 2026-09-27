package infraconfig

import (
	"os"
	"testing"

	"github.com/goccy/go-yaml"
)

func TestMonitoringYAMLIsValid(t *testing.T) {
	paths := []string{
		"../../../../docker-compose.yml",
		"../../../../monitoring/tempo.yml",
		"../../../../monitoring/grafana/provisioning/datasources/prometheus.yml",
	}
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(content, &document); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
	}
}
