package otelkafkago

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
)

// Option applies options to a configuration.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a generic way to set an option using a func.
type OptionFunc func(*Config)

// Apply applies the configuration option.
func (o OptionFunc) Apply(c *Config) {
	o(c)
}

// WithTracerProvider returns an Option that sets the TracerProvider used for
// a configuration.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return OptionFunc(func(c *Config) {
		c.Tracer = tp.Tracer(
			c.instName,
			trace.WithInstrumentationVersion(version),
			trace.WithSchemaURL(semconv.SchemaURL),
		)
	})
}

// WithAttributes returns an Option that appends attr to the attributes set
// for every span created.
func WithAttributes(attr []attribute.KeyValue) Option {
	return OptionFunc(func(c *Config) {
		c.DefaultStartOpts = append(
			c.DefaultStartOpts,
			trace.WithAttributes(attr...),
		)
	})
}

// WithPropagator returns an Option that sets p as the TextMapPropagator used
// when propagating a span context.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return OptionFunc(func(c *Config) {
		c.Propagator = p
	})
}
