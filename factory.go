package natsexporter

import (
	"context"
	"fmt"
	"natsexporter/internal/metadata"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// NewFactory creates a factory for OTLP exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		// exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		// exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability))
}

type NatsExporter interface {
	component.Component
	// consumeTraces(_ context.Context, td ptrace.Traces) error
	// consumeMetrics(_ context.Context, md pmetric.Metrics) error
	consumeLogs(_ context.Context, ld plog.Logs) error
}

func createDefaultConfig() component.Config {
	return &Config{
		Path: "default_path",
	}
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	fe := getOrCreateFileExporter(cfg, set.Logger)
	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		fe.consumeLogs,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func getOrCreateFileExporter(cfg component.Config, logger *zap.Logger) NatsExporter {
	conf := cfg.(*Config)
	return newFileExporter(conf, logger)
}

func newFileExporter(conf *Config, logger *zap.Logger) NatsExporter {
	return &natsExporter{
		conf:   conf,
		logger: logger,
	}
}

type natsExporter struct {
	conf   *Config
	logger *zap.Logger
}

func (n *natsExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	fmt.Printf("natsExporter.consumeLogs(), ld=%v", ld)
	return nil
}

func (n *natsExporter) Start(_ context.Context, host component.Host) error {
	fmt.Println("natsExporter.Start()")
	return nil
}

func (n *natsExporter) Shutdown(context.Context) error {
	fmt.Println("natsExporter.Shutdown()")
	return nil
}
