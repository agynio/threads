package config

import (
	"fmt"
	"os"
)

type Config struct {
	GRPCAddress          string
	DatabaseURL          string
	NotificationsAddress string
	IdentityAddress      string
	AuthorizationAddress string
}

func FromEnv() (Config, error) {
	cfg := Config{}
	cfg.GRPCAddress = os.Getenv("GRPC_ADDRESS")
	if cfg.GRPCAddress == "" {
		cfg.GRPCAddress = ":50051"
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}
	cfg.NotificationsAddress = os.Getenv("NOTIFICATIONS_ADDRESS")
	if cfg.NotificationsAddress == "" {
		return Config{}, fmt.Errorf("NOTIFICATIONS_ADDRESS must be set")
	}
	cfg.IdentityAddress = os.Getenv("IDENTITY_ADDRESS")
	if cfg.IdentityAddress == "" {
		return Config{}, fmt.Errorf("IDENTITY_ADDRESS must be set")
	}
	cfg.AuthorizationAddress = os.Getenv("AUTHORIZATION_ADDRESS")
	if cfg.AuthorizationAddress == "" {
		return Config{}, fmt.Errorf("AUTHORIZATION_ADDRESS must be set")
	}
	return cfg, nil
}
