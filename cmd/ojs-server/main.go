package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	ojsgrpc "github.com/openjobspec/ojs-backend-kafka/internal/grpc"
	kafkabackend "github.com/openjobspec/ojs-backend-kafka/internal/kafka"
	"github.com/openjobspec/ojs-backend-kafka/internal/metrics"
	"github.com/openjobspec/ojs-backend-kafka/internal/scheduler"
	"github.com/openjobspec/ojs-backend-kafka/internal/server"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := server.LoadConfig()

	// Connect to Redis state store
	store, err := state.NewRedisStore(cfg.RedisURL)
	if err != nil {
		slog.Error("failed to connect to Redis state store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	slog.Info("connected to Redis state store", "url", cfg.RedisURL)

	// Create Kafka producer client
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("failed to create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	// Verify Kafka connectivity
	if err := kafkaClient.Ping(context.Background()); err != nil {
		slog.Error("failed to connect to Kafka", "brokers", cfg.KafkaBrokers, "error", err)
		os.Exit(1)
	}
	slog.Info("connected to Kafka", "brokers", cfg.KafkaBrokers)

	// Initialize Prometheus server info metric
	metrics.Init(core.OJSVersion, "kafka")

	// Create producer and backend
	producer := kafkabackend.NewProducer(kafkaClient, cfg.UseQueueKey, cfg.EventsEnabled)
	backend := kafkabackend.New(store, producer)

	// Start background scheduler
	sched := scheduler.New(backend)
	sched.Start()
	defer sched.Stop()

	// Create HTTP server
	router := server.NewRouter(backend, cfg)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Start server
	go func() {
		slog.Info("OJS Kafka server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Start gRPC server
	grpcServer := grpc.NewServer()
	ojsgrpc.Register(grpcServer, backend)

	go func() {
		lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
		if err != nil {
			slog.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
			os.Exit(1)
		}
		slog.Info("OJS Kafka gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	sched.Stop()
	grpcServer.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	slog.Info("server stopped")
}
