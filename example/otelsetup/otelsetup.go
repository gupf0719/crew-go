// Package otelsetup 是示例程序用的 OpenTelemetry SDK 装配：按参数建 TracerProvider / MeterProvider，
// 设成全局，再交给 observability.New() 使用。生产应用按自己的需要配 SDK（导出器、采样、Resource），
// 这里只是最常见的一种写法。
package otelsetup

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config 描述要装哪些导出器。都不配时只装 propagator，span 与指标不导出（API 走 no-op 之外的 SDK 但没有出口）。
type Config struct {
	ServiceName string // 写进 Resource 的 service.name
	// TraceEndpoint / MetricsEndpoint 是 OTLP gRPC 地址（如 127.0.0.1:4317），空则不导出对应信号。
	TraceEndpoint   string
	MetricsEndpoint string
	// Insecure 用明文 gRPC（本地 collector 常见）。
	Insecure bool
	// Stdout 把 span 打印到 StdoutWriter（默认 os.Stdout），本地看 trace 树用。
	Stdout       bool
	StdoutWriter io.Writer
}

// Setup 建并装好全局 provider，返回 shutdown（退出前调，把缓冲里的 span / 指标刷出去）。
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	res := resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName))
	var shutdowns []func(context.Context) error

	topts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if cfg.TraceEndpoint != "" {
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.TraceEndpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, err
		}
		topts = append(topts, sdktrace.WithBatcher(exp))
	}
	if cfg.Stdout {
		w := cfg.StdoutWriter
		if w == nil {
			w = os.Stdout
		}
		exp, err := stdouttrace.New(stdouttrace.WithWriter(w), stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, err
		}
		topts = append(topts, sdktrace.WithSyncer(exp))
	}
	tp := sdktrace.NewTracerProvider(topts...)
	shutdowns = append(shutdowns, tp.Shutdown)
	otel.SetTracerProvider(tp)

	mopts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	if cfg.MetricsEndpoint != "" {
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.MetricsEndpoint)}
		if cfg.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			return nil, err
		}
		mopts = append(mopts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))))
	}
	mp := sdkmetric.NewMeterProvider(mopts...)
	shutdowns = append(shutdowns, mp.Shutdown)
	otel.SetMeterProvider(mp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func(ctx context.Context) error {
		var errs []error
		for _, f := range shutdowns {
			errs = append(errs, f(ctx))
		}
		return errors.Join(errs...)
	}, nil
}
