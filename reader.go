package otelkafkago

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/segmentio/kafka-go"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
	"go.opentelemetry.io/otel/trace"
)

type Reader struct {
	R          *kafka.Reader
	cfg        *Config
	activeSpan unsafe.Pointer
}

type readerSpan struct {
	otelSpan trace.Span
}

func WrapReader(c *kafka.Reader) *Reader {
	return wrapReader(c, newConfig())
}

// NewReader calls kafka.NewReader and wraps the resulting Consumer with
// tracing instrumentation.
func NewReader(r *kafka.Reader) (*Reader, error) {

	cfg := newConfig()
	// The kafka Consumer does not expose consumer group, but we want to
	// include this in the span attributes.

	cfg.DefaultStartOpts = append(
		cfg.DefaultStartOpts,
		trace.WithAttributes(
			semconv.MessagingKafkaConsumerGroupKey.String(r.Config().GroupID),
		),
	)

	return wrapReader(r, cfg), nil
}

func wrapReader(r *kafka.Reader, cfg *Config) *Reader {
	// Common attributes for all spans this consumer will produce.
	cfg.DefaultStartOpts = append(
		cfg.DefaultStartOpts,
		trace.WithAttributes(
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingOperationReceive,
			semconv.MessagingKafkaClientIDKey.String(r.Stats().ClientID),
		),
	)
	wrapped := &Reader{
		R:   r,
		cfg: cfg,
		// Set an empty spanHolder to set the activeSpan to empty and ensure that
		// the unsafe.Pointer is set to the correct type.
		activeSpan: unsafe.Pointer(&readerSpan{}),
	}
	return wrapped
}

func (s readerSpan) End(options ...trace.SpanEndOption) {
	if s.otelSpan != nil {
		s.otelSpan.End(options...)
	}
}

func (r *Reader) startSpan(msg *kafka.Message) readerSpan {
	carrier := NewMessageCarrier(msg)
	psc := r.cfg.Propagator.Extract(context.Background(), carrier)

	const base10 = 10
	offset := strconv.FormatInt(int64(msg.Offset), base10)
	opts := r.cfg.MergedSpanStartOptions(
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(msg.Topic),
			semconv.MessagingMessageIDKey.String(offset),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	name := fmt.Sprintf("%s receive", msg.Topic)
	ctx, otelSpan := r.cfg.Tracer.Start(psc, name, opts...)

	// Inject the current span into the original message so it can be used to
	// propagate the span.
	r.cfg.Propagator.Inject(ctx, carrier)

	return readerSpan{otelSpan: otelSpan}
}

// Close calls the underlying Consumer.Close and if polling is enabled, ends
// any remaining span.
func (r *Reader) Close() error {
	err := r.R.Close()
	(*readerSpan)(atomic.LoadPointer(&r.activeSpan)).End()
	return err
}

func (r *Reader) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	endTime := time.Now()
	msg, err := r.R.ReadMessage(ctx)
	if err == nil {
		s := r.startSpan(&msg)
		active := atomic.SwapPointer(&r.activeSpan, unsafe.Pointer(&s))
		(*readerSpan)(active).End(trace.WithTimestamp(endTime))
		s.End()
	}
	return &msg, err
}
