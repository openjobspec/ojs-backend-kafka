package server

import (
	"strings"

	commonconfig "github.com/openjobspec/ojs-go-backend-common/config"
)

// Config holds server configuration from environment variables.
type Config struct {
	commonconfig.BaseConfig
	KafkaBrokers  []string
	RedisURL      string
	UseQueueKey   bool
	EventsEnabled bool
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	brokers := commonconfig.GetEnv("KAFKA_BROKERS", "localhost:9092")
	return Config{
		BaseConfig:    commonconfig.LoadBaseConfig(),
		KafkaBrokers:  strings.Split(brokers, ","),
		RedisURL:      commonconfig.GetEnv("REDIS_URL", "redis://localhost:6379"),
		UseQueueKey:   commonconfig.GetEnvBool("OJS_KAFKA_USE_QUEUE_KEY", false),
		EventsEnabled: commonconfig.GetEnvBool("OJS_KAFKA_EVENTS_ENABLED", true),
	}
}
