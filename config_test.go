package otelkafkago

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
	"go.opentelemetry.io/otel/trace"
)

type testTextMapProp struct{}

var _ propagation.TextMapPropagator = (*testTextMapProp)(nil)

func (*testTextMapProp) Inject(context.Context, propagation.TextMapCarrier) {}

func (*testTextMapProp) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (*testTextMapProp) Fields() []string { return nil }

func TestConfigDefaultPropagator(t *testing.T) {
	c := newConfig()
	expected := otel.GetTextMapPropagator()
	assert.Same(t, expected, c.Propagator)
}

func TestNewConfig(t *testing.T) {
	expectedConfig := &Config{
		instName: instrumentationName,
		Tracer: otel.Tracer(
			instrumentationName,
			trace.WithInstrumentationVersion(version),
			trace.WithSchemaURL(semconv.SchemaURL),
		),
		Propagator:       otel.GetTextMapPropagator(),
		DefaultStartOpts: []trace.SpanStartOption{trace.WithAttributes(semconv.MessagingSystemKey.String("kafka"))},
	}
	newConfig := newConfig()

	assert.Equal(t, expectedConfig.instName, newConfig.instName)
	assert.Equal(t, expectedConfig.Tracer, newConfig.Tracer)
	assert.Equal(t, expectedConfig.Propagator, newConfig.Propagator)
	assert.Equal(t, len(expectedConfig.DefaultStartOpts), len(newConfig.DefaultStartOpts))

	for i, expected := range expectedConfig.DefaultStartOpts {
		assert.Equal(t, expected, newConfig.DefaultStartOpts[i])
	}
}
