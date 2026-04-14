package config

import "testing"

func TestFromEnvDefaults(t *testing.T) {
	t.Setenv("GRPC_ADDRESS", "")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/threads")
	t.Setenv("NOTIFICATIONS_ADDRESS", "")
	t.Setenv("IDENTITY_ADDRESS", "")
	t.Setenv("METERING_SERVICE_ADDRESS", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv returned error: %v", err)
	}
	if cfg.GRPCAddress != ":50051" {
		t.Fatalf("expected default GRPC address :50051, got %q", cfg.GRPCAddress)
	}
	if cfg.DatabaseURL != "postgres://user:pass@localhost:5432/threads" {
		t.Fatalf("expected database url to be set, got %q", cfg.DatabaseURL)
	}
	if cfg.NotificationsAddress != "notifications:50051" {
		t.Fatalf("expected notifications address notifications:50051, got %q", cfg.NotificationsAddress)
	}
	if cfg.IdentityAddress != "identity:50051" {
		t.Fatalf("expected identity address identity:50051, got %q", cfg.IdentityAddress)
	}
	if cfg.MeteringServiceAddress != "metering:50051" {
		t.Fatalf("expected metering address metering:50051, got %q", cfg.MeteringServiceAddress)
	}
}

func TestFromEnvOverrides(t *testing.T) {
	t.Setenv("GRPC_ADDRESS", "0.0.0.0:9999")
	t.Setenv("DATABASE_URL", "postgres://user:pass@db:5432/threads")
	t.Setenv("NOTIFICATIONS_ADDRESS", "notifications.internal:6000")
	t.Setenv("IDENTITY_ADDRESS", "identity.internal:6001")
	t.Setenv("METERING_SERVICE_ADDRESS", "metering.internal:6002")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv returned error: %v", err)
	}
	if cfg.GRPCAddress != "0.0.0.0:9999" {
		t.Fatalf("expected grpc address 0.0.0.0:9999, got %q", cfg.GRPCAddress)
	}
	if cfg.NotificationsAddress != "notifications.internal:6000" {
		t.Fatalf("expected notifications address override, got %q", cfg.NotificationsAddress)
	}
	if cfg.IdentityAddress != "identity.internal:6001" {
		t.Fatalf("expected identity address override, got %q", cfg.IdentityAddress)
	}
	if cfg.MeteringServiceAddress != "metering.internal:6002" {
		t.Fatalf("expected metering address override, got %q", cfg.MeteringServiceAddress)
	}
}

func TestFromEnvRequiresDatabaseURL(t *testing.T) {
	t.Setenv("GRPC_ADDRESS", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("NOTIFICATIONS_ADDRESS", "")
	t.Setenv("IDENTITY_ADDRESS", "")
	t.Setenv("METERING_SERVICE_ADDRESS", "")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error when DATABASE_URL is missing")
	}
}
