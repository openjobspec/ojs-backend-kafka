package server

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds server configuration from environment variables.
type Config struct {
	Port          string
	GRPCPort      string
	KafkaBrokers  []string
	RedisURL      string
	UseQueueKey   bool
	EventsEnabled bool
	APIKey        string

	// HTTP server timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// Shutdown drain timeout
	ShutdownTimeout time.Duration
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	return Config{
		Port:          getEnv("OJS_PORT", "8080"),
		GRPCPort:      getEnv("OJS_GRPC_PORT", "9090"),
		KafkaBrokers:  strings.Split(brokers, ","),
		RedisURL:      getEnv("REDIS_URL", "redis://localhost:6379"),
		UseQueueKey:   getEnvBool("OJS_KAFKA_USE_QUEUE_KEY", false),
		EventsEnabled: getEnvBool("OJS_KAFKA_EVENTS_ENABLED", true),
		APIKey:        getEnv("OJS_API_KEY", ""),

		ReadTimeout:  getDurationEnv("OJS_READ_TIMEOUT", 30*time.Second),
		WriteTimeout: getDurationEnv("OJS_WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:  getDurationEnv("OJS_IDLE_TIMEOUT", 120*time.Second),

		ShutdownTimeout: getDurationEnv("OJS_SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		switch strings.ToLower(val) {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
