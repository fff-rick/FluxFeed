package infraconfig

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml"
)

var ErrEmptyConfigPath = errors.New("config file path is empty")
var ErrReadConfigFailed = errors.New("read config file failed")
var ErrUnmarshalConfigFailed = errors.New("unmarshal config failed")
var ErrInvalidProductionConfig = errors.New("invalid production config")

// LoadConfig 读取 YAML 配置文件，并反序列化为应用启动配置。
func LoadConfig(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrEmptyConfigPath
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, ErrReadConfigFailed
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		return nil, ErrUnmarshalConfigFailed
	}
	if err := applyEnvironment(cfg); err != nil {
		return nil, err
	}
	if err := validateProduction(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyEnvironment(cfg *Config) error {
	setString(&cfg.Environment, "FLUXFEED_ENV")
	setString(&cfg.JWT.Secret, "FLUXFEED_JWT_SECRET")
	setString(&cfg.Internal.Token, "FLUXFEED_INTERNAL_TOKEN")
	setString(&cfg.Admin.Account, "ADMIN")
	setString(&cfg.Admin.Password, "ADMIN_PW")
	setString(&cfg.Database.Host, "FLUXFEED_DATABASE_HOST")
	setString(&cfg.Database.User, "FLUXFEED_DATABASE_USER")
	setString(&cfg.Database.Password, "FLUXFEED_DATABASE_PASSWORD")
	setString(&cfg.Database.Name, "FLUXFEED_DATABASE_NAME")
	setString(&cfg.Database.TLS, "FLUXFEED_DATABASE_TLS")
	setString(&cfg.Redis.Addr, "FLUXFEED_REDIS_ADDR")
	setString(&cfg.Redis.Password, "FLUXFEED_REDIS_PASSWORD")
	setString(&cfg.Redis.TLSServerName, "FLUXFEED_REDIS_TLS_SERVER_NAME")
	setString(&cfg.Kafka.Username, "FLUXFEED_KAFKA_USERNAME")
	setString(&cfg.Kafka.Password, "FLUXFEED_KAFKA_PASSWORD")
	setString(&cfg.Kafka.TLSServerName, "FLUXFEED_KAFKA_TLS_SERVER_NAME")
	setString(&cfg.RabbitMQ.URL, "FLUXFEED_RABBITMQ_URL")
	if (cfg.Admin.Account == "") != (cfg.Admin.Password == "") {
		return errors.New("ADMIN and ADMIN_PW must be configured together")
	}

	if raw, ok := os.LookupEnv("FLUXFEED_KAFKA_BROKERS"); ok {
		cfg.Kafka.Brokers = splitNonEmpty(raw)
	}
	for name, target := range map[string]*int{
		"FLUXFEED_DATABASE_PORT": &cfg.Database.Port,
		"FLUXFEED_REDIS_DB":      &cfg.Redis.DB,
	} {
		if raw, ok := os.LookupEnv(name); ok {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err != nil {
				return fmt.Errorf("%s must be an integer", name)
			}
			*target = value
		}
	}
	for name, target := range map[string]*bool{
		"FLUXFEED_REDIS_TLS": &cfg.Redis.TLS,
		"FLUXFEED_KAFKA_TLS": &cfg.Kafka.TLS,
	} {
		if raw, ok := os.LookupEnv(name); ok {
			value, err := strconv.ParseBool(strings.TrimSpace(raw))
			if err != nil {
				return fmt.Errorf("%s must be a boolean", name)
			}
			*target = value
		}
	}
	return nil
}

func validateProduction(cfg *Config) error {
	if !strings.EqualFold(strings.TrimSpace(cfg.Environment), "production") {
		return nil
	}
	invalid := make([]string, 0)
	if weakSecret(cfg.JWT.Secret, "replace-with-strong-secret") {
		invalid = append(invalid, "FLUXFEED_JWT_SECRET must contain at least 32 characters")
	}
	if weakSecret(cfg.Internal.Token, "replace-with-internal-token") {
		invalid = append(invalid, "FLUXFEED_INTERNAL_TOKEN must contain at least 32 characters")
	}
	if strings.TrimSpace(cfg.Database.Password) == "" {
		invalid = append(invalid, "FLUXFEED_DATABASE_PASSWORD is required")
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Database.User), "root") || strings.TrimSpace(cfg.Database.User) == "" {
		invalid = append(invalid, "FLUXFEED_DATABASE_USER must be a non-root account")
	}
	if tlsMode := strings.ToLower(strings.TrimSpace(cfg.Database.TLS)); tlsMode == "" || tlsMode == "false" || tlsMode == "skip-verify" || tlsMode == "preferred" {
		invalid = append(invalid, "FLUXFEED_DATABASE_TLS must enable certificate-verified TLS")
	}
	if strings.TrimSpace(cfg.Redis.Password) == "" {
		invalid = append(invalid, "FLUXFEED_REDIS_PASSWORD is required")
	}
	if !cfg.Redis.TLS {
		invalid = append(invalid, "FLUXFEED_REDIS_TLS must be true")
	}
	if len(cfg.Kafka.Brokers) > 0 && (strings.TrimSpace(cfg.Kafka.Username) == "" || strings.TrimSpace(cfg.Kafka.Password) == "") {
		invalid = append(invalid, "Kafka credentials are required when Kafka is enabled")
	}
	if len(cfg.Kafka.Brokers) > 0 && !cfg.Kafka.TLS {
		invalid = append(invalid, "FLUXFEED_KAFKA_TLS must be true when Kafka is enabled")
	}
	if raw := strings.TrimSpace(cfg.RabbitMQ.URL); raw != "" {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.User == nil {
			invalid = append(invalid, "FLUXFEED_RABBITMQ_URL must contain credentials")
		} else {
			password, hasPassword := parsed.User.Password()
			if strings.EqualFold(parsed.User.Username(), "guest") || !hasPassword || password == "" {
				invalid = append(invalid, "RabbitMQ must not use guest or an empty password")
			}
			if !strings.EqualFold(parsed.Scheme, "amqps") {
				invalid = append(invalid, "RabbitMQ must use amqps in production")
			}
		}
	}
	if len(invalid) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidProductionConfig, strings.Join(invalid, "; "))
	}
	return nil
}

func setString(target *string, name string) {
	if value, ok := os.LookupEnv(name); ok {
		*target = strings.TrimSpace(value)
	}
}

func splitNonEmpty(raw string) []string {
	values := make([]string, 0)
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func weakSecret(value string, forbidden string) bool {
	value = strings.TrimSpace(value)
	return len(value) < 32 || value == forbidden
}
