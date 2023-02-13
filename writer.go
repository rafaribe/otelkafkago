package otelkafkago

import (
	"context"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
	"go.opentelemetry.io/otel/trace"
)

type Writer struct {
	W   *kafka.Writer
	cfg *Config
}

// NewWriter wraps the resulting Writer with OpenTelemetry instrumentation
func NewWriter(w *kafka.Writer) (*Writer, error) {
	return WrapWriter(w), nil
}

// WrapProducer wraps a kafka.Producer so that any produced events are traced.
func WrapWriter(w *kafka.Writer, opts ...Option) *Writer {
	cfg := newConfig()
	// Common attributes for all spans this producer will produce.
	cfg.DefaultStartOpts = append(
		cfg.DefaultStartOpts,
		trace.WithAttributes(
			semconv.MessagingDestinationKindTopic,
		),
	)
	wrapped := &Writer{
		W:   w,
		cfg: cfg,
	}
	return wrapped
}

func (w *Writer) startSpan(msg *kafka.Message) trace.Span {
	carrier := NewMessageCarrier(msg)
	psc := w.cfg.Propagator.Extract(context.Background(), carrier)

	const base10 = 10
	offset := strconv.FormatInt(int64(msg.Offset), base10)
	opts := w.cfg.MergedSpanStartOptions(
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(msg.Topic),
			semconv.MessagingMessageIDKey.String(offset),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)

	name := fmt.Sprintf("%s send", msg.Topic)
	ctx, span := w.cfg.Tracer.Start(psc, name, opts...)

	w.cfg.Propagator.Inject(ctx, carrier)
	return span
}

func (w *Writer) Close() {
	w.W.Close()
}

func (w *Writer) WriteMessages(ctx context.Context, msg kafka.Message) error {
	span := w.startSpan(&msg)
	err := w.W.WriteMessages(ctx, msg)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	return err
}
