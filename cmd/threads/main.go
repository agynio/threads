package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	agentsv1 "github.com/agynio/threads/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	meteringv1 "github.com/agynio/threads/.gen/go/agynio/api/metering/v1"
	notificationsv1 "github.com/agynio/threads/.gen/go/agynio/api/notifications/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agynio/threads/internal/config"
	"github.com/agynio/threads/internal/db"
	"github.com/agynio/threads/internal/metering"
	"github.com/agynio/threads/internal/notifier"
	"github.com/agynio/threads/internal/server"
	"github.com/agynio/threads/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("threads: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer notificationsConn.Close()

	identityConn, err := grpc.DialContext(ctx, cfg.IdentityAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial identity: %w", err)
	}
	defer identityConn.Close()

	agentsConn, err := grpc.DialContext(ctx, cfg.AgentsServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer agentsConn.Close()

	meteringConn, err := grpc.DialContext(ctx, cfg.MeteringServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial metering: %w", err)
	}
	defer meteringConn.Close()

	threadsServer := grpc.NewServer()
	threadsv1.RegisterThreadsServiceServer(
		threadsServer,
		server.New(
			store.NewStore(pool),
			notifier.New(notificationsv1.NewNotificationsServiceClient(notificationsConn)),
			identityv1.NewIdentityServiceClient(identityConn),
			agentsv1.NewAgentsServiceClient(agentsConn),
			metering.New(meteringv1.NewMeteringServiceClient(meteringConn)),
		),
	)

	lis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddress, err)
	}

	go func() {
		<-ctx.Done()
		threadsServer.GracefulStop()
	}()

	log.Printf("ThreadsService listening on %s", cfg.GRPCAddress)
	if err := threadsServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
