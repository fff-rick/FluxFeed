package infraconfig

import (
	"errors"
	"strings"
	"testing"
)

func TestKafkaConfigLoadsFromApplicationConfigs(t *testing.T) {
	for _, path := range []string{
		"../../../configs/config.yaml",
		"../../../configs/config.docker.yaml",
		"../../../configs/config.performance.yaml",
	} {
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig(%s): %v", path, err)
		}
		if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.VideoTopic == "" ||
			cfg.Kafka.FanoutGroup == cfg.Kafka.EmbeddingGroup || cfg.Kafka.RecommendationGroup == cfg.Kafka.FeatureGroup ||
			cfg.Governance.RequestTimeout == "" || cfg.Governance.RateLimitRPS <= 0 {
			t.Fatalf("unexpected Kafka config from %s: %+v", path, cfg.Kafka)
		}
	}
}

func TestEnvironmentOverridesSecrets(t *testing.T) {
	t.Setenv("FLUXFEED_JWT_SECRET", "jwt-from-environment")
	t.Setenv("FLUXFEED_INTERNAL_TOKEN", "internal-from-environment")
	t.Setenv("FLUXFEED_DATABASE_PASSWORD", "database-from-environment")
	t.Setenv("FLUXFEED_REDIS_PASSWORD", "redis-from-environment")
	t.Setenv("FLUXFEED_RABBITMQ_URL", "amqp://fluxfeed:secret@rabbitmq:5672/")
	t.Setenv("ADMIN", "bootstrap-admin")
	t.Setenv("ADMIN_PW", "admin-password")

	cfg := &Config{}
	if err := applyEnvironment(cfg); err != nil {
		t.Fatalf("applyEnvironment: %v", err)
	}
	if cfg.JWT.Secret != "jwt-from-environment" || cfg.Internal.Token != "internal-from-environment" ||
		cfg.Database.Password != "database-from-environment" || cfg.Redis.Password != "redis-from-environment" ||
		cfg.RabbitMQ.URL != "amqp://fluxfeed:secret@rabbitmq:5672/" || cfg.Admin.Account != "bootstrap-admin" || cfg.Admin.Password != "admin-password" {
		t.Fatalf("environment secrets were not applied: %+v", cfg)
	}
}

func TestAdminEnvironmentMustBeComplete(t *testing.T) {
	t.Setenv("ADMIN", "bootstrap-admin")
	t.Setenv("ADMIN_PW", "")
	if err := applyEnvironment(&Config{}); err == nil {
		t.Fatal("expected incomplete admin configuration to fail")
	}
}

func TestProductionRejectsWeakOrInsecureConfig(t *testing.T) {
	cfg := &Config{
		Environment: "production",
		JWT:         JWTConfig{Secret: "short"},
		Internal:    InternalConfig{Token: "short"},
		Database:    DatabaseConfig{User: "root"},
		RabbitMQ:    RabbitMQConfig{URL: "amqp://guest:guest@rabbitmq:5672/"},
	}
	if err := validateProduction(cfg); !errors.Is(err, ErrInvalidProductionConfig) {
		t.Fatalf("expected production config error, got %v", err)
	}
}

func TestProductionAcceptsInjectedSecureConfig(t *testing.T) {
	strongSecret := strings.Repeat("a", 32)
	cfg := &Config{
		Environment: "production",
		JWT:         JWTConfig{Secret: strongSecret},
		Internal:    InternalConfig{Token: strongSecret},
		Database:    DatabaseConfig{User: "fluxfeed", Password: "database-secret", TLS: "true"},
		Redis:       RedisConfig{Password: "redis-secret", TLS: true},
		Kafka:       KafkaConfig{Brokers: []string{"kafka:9093"}, Username: "fluxfeed", Password: "kafka-secret", TLS: true},
		RabbitMQ:    RabbitMQConfig{URL: "amqps://fluxfeed:rabbit-secret@rabbitmq:5671/"},
	}
	if err := validateProduction(cfg); err != nil {
		t.Fatalf("validateProduction: %v", err)
	}
}
