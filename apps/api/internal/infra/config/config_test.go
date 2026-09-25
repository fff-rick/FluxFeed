package infraconfig

import "testing"

func TestKafkaConfigLoadsFromApplicationConfigs(t *testing.T) {
	for _, path := range []string{"../../../configs/config.yaml", "../../../configs/config.docker.yaml"} {
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig(%s): %v", path, err)
		}
		if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.VideoTopic == "" ||
			cfg.Kafka.FanoutGroup == cfg.Kafka.EmbeddingGroup || cfg.Kafka.RecommendationGroup == cfg.Kafka.FeatureGroup {
			t.Fatalf("unexpected Kafka config from %s: %+v", path, cfg.Kafka)
		}
	}
}
