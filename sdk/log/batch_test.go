// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package log // import "go.opentelemetry.io/otel/sdk/log"

import (
	"bytes"
	"context"
	stdlog "log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/stdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/log"
)

type syncBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (b *syncBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}

func TestEmptyBatchConfig(t *testing.T) {
	assert.NotPanics(t, func() {
		var bp BatchProcessor
		ctx := context.Background()
		record := new(Record)
		assert.NoError(t, bp.OnEmit(ctx, record), "OnEmit")
		assert.NoError(t, bp.ForceFlush(ctx), "ForceFlush")
		assert.NoError(t, bp.Shutdown(ctx), "Shutdown")
	})
}

func TestNewBatchConfig(t *testing.T) {
	testcases := []struct {
		name    string
		envars  map[string]string
		options []BatchProcessorOption
		want    batchConfig
	}{
		{
			name: "Defaults",
			want: batchConfig{
				maxQSize:        newSetting(dfltMaxQSize),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(dfltExpMaxBatchSize),
			},
		},
		{
			name: "Options",
			options: []BatchProcessorOption{
				WithMaxQueueSize(10),
				WithExportInterval(time.Microsecond),
				WithExportTimeout(time.Hour),
				WithExportMaxBatchSize(2),
				WithExportBufferSize(3),
			},
			want: batchConfig{
				maxQSize:        newSetting(10),
				expInterval:     newSetting(time.Microsecond),
				expTimeout:      newSetting(time.Hour),
				expMaxBatchSize: newSetting(2),
			},
		},
		{
			name: "Environment",
			envars: map[string]string{
				envarMaxQSize:        strconv.Itoa(10),
				envarExpInterval:     strconv.Itoa(100),
				envarExpTimeout:      strconv.Itoa(1000),
				envarExpMaxBatchSize: strconv.Itoa(1),
			},
			want: batchConfig{
				maxQSize:        newSetting(10),
				expInterval:     newSetting(100 * time.Millisecond),
				expTimeout:      newSetting(1000 * time.Millisecond),
				expMaxBatchSize: newSetting(1),
			},
		},
		{
			name: "InvalidOptions",
			options: []BatchProcessorOption{
				WithMaxQueueSize(-11),
				WithExportInterval(-1 * time.Microsecond),
				WithExportTimeout(-1 * time.Hour),
				WithExportMaxBatchSize(-2),
				WithExportBufferSize(-2),
			},
			want: batchConfig{
				maxQSize:        newSetting(dfltMaxQSize),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(dfltExpMaxBatchSize),
			},
		},
		{
			name: "InvalidEnvironment",
			envars: map[string]string{
				envarMaxQSize:        "-1",
				envarExpInterval:     "-1",
				envarExpTimeout:      "-1",
				envarExpMaxBatchSize: "-1",
			},
			want: batchConfig{
				maxQSize:        newSetting(dfltMaxQSize),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(dfltExpMaxBatchSize),
			},
		},
		{
			name: "Precedence",
			envars: map[string]string{
				envarMaxQSize:        strconv.Itoa(1),
				envarExpInterval:     strconv.Itoa(100),
				envarExpTimeout:      strconv.Itoa(1000),
				envarExpMaxBatchSize: strconv.Itoa(10),
			},
			options: []BatchProcessorOption{
				// These override the environment variables.
				WithMaxQueueSize(3),
				WithExportInterval(time.Microsecond),
				WithExportTimeout(time.Hour),
				WithExportMaxBatchSize(2),
				WithExportBufferSize(2),
			},
			want: batchConfig{
				maxQSize:        newSetting(3),
				expInterval:     newSetting(time.Microsecond),
				expTimeout:      newSetting(time.Hour),
				expMaxBatchSize: newSetting(2),
			},
		},
		{
			name: "BatchLessThanOrEqualToQSize",
			options: []BatchProcessorOption{
				WithMaxQueueSize(1),
				WithExportMaxBatchSize(10),
				WithExportBufferSize(3),
			},
			want: batchConfig{
				maxQSize:        newSetting(1),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(1),
			},
		},
		{
			name: "DefaultBatchLessThanOrEqualToOptionQSize",
			options: []BatchProcessorOption{
				WithMaxQueueSize(1),
			},
			want: batchConfig{
				maxQSize:        newSetting(1),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(1),
			},
		},
		{
			name: "DefaultBatchLessThanOrEqualToEnvironmentQSize",
			envars: map[string]string{
				envarMaxQSize: "1",
			},
			want: batchConfig{
				maxQSize:        newSetting(1),
				expInterval:     newSetting(dfltExpInterval),
				expTimeout:      newSetting(dfltExpTimeout),
				expMaxBatchSize: newSetting(1),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.envars {
				t.Setenv(key, value)
			}
			assert.Equal(t, tc.want, newBatchConfig(tc.options))
		})
	}
}

func TestBatchProcessor(t *testing.T) {
	ctx := context.Background()

	t.Run("NilExporter", func(t *testing.T) {
		var b *BatchProcessor
		assert.NotPanics(t, func() { b = NewBatchProcessor(nil) })
		assert.NoError(t, b.OnEmit(ctx, new(Record)))
		assert.NoError(t, b.ForceFlush(ctx))
		assert.NoError(t, b.Shutdown(ctx))
	})

	t.Run("Polling", func(t *testing.T) {
		e := &testExporter{}
		const size = 15
		b := NewBatchProcessor(
			e,
			WithMaxQueueSize(2*size),
			WithExportMaxBatchSize(2*size),
			WithExportInterval(time.Nanosecond),
			WithExportTimeout(time.Hour),
		)
		defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
		for i := 0; i < size; i++ {
			assert.NoError(t, b.OnEmit(ctx, new(Record)))
		}
		var got []Record
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, r := range e.Records() {
				got = append(got, r...)
			}
			assert.Len(c, got, size)
		}, 2*time.Second, time.Microsecond)
		_ = b.Shutdown(ctx)
	})

	t.Run("OnEmit", func(t *testing.T) {
		const batch = 10
		e := &testExporter{}
		b := NewBatchProcessor(
			e,
			WithMaxQueueSize(10*batch),
			WithExportMaxBatchSize(batch),
			WithExportInterval(time.Hour),
			WithExportTimeout(time.Hour),
		)
		defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
		for i := 0; i < 10*batch; i++ {
			assert.NoError(t, b.OnEmit(ctx, new(Record)))
		}
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, e.ExportN(), 1)
		}, 2*time.Second, time.Microsecond, "multi-batch flush")

		assert.NoError(t, b.Shutdown(ctx))
		assert.GreaterOrEqual(t, e.ExportN(), 10)
	})

	t.Run("ScheduledExportError", func(t *testing.T) {
		original := otel.GetErrorHandler()
		handled := make(chan error, 1)
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			select {
			case handled <- err:
			default:
			}
		}))
		t.Cleanup(func() { otel.SetErrorHandler(original) })

		e := &testExporter{
			ExportFunc: func(context.Context, []Record) error {
				return assert.AnError
			},
		}
		b := NewBatchProcessor(
			e,
			WithMaxQueueSize(1),
			WithExportInterval(time.Hour),
		)
		defer func() { assert.NoError(t, b.Shutdown(ctx)) }()

		require.NoError(t, b.OnEmit(ctx, new(Record)))
		select {
		case err := <-handled:
			assert.ErrorIs(t, err, assert.AnError)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for scheduled export error")
		}
	})

	t.Run("RetriggerFlushNonBlocking", func(t *testing.T) {
		e := &testExporter{}
		e.ExportTrigger = make(chan struct{})
		var release sync.Once
		releaseExport := func() { release.Do(func() { close(e.ExportTrigger) }) }

		const batch = 10
		b := NewBatchProcessor(
			e,
			WithMaxQueueSize(3*batch),
			WithExportMaxBatchSize(batch),
			WithExportInterval(time.Hour),
			WithExportTimeout(time.Hour),
		)
		defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
		defer releaseExport()
		for i := 0; i < 2*batch; i++ {
			assert.NoError(t, b.OnEmit(ctx, new(Record)))
		}

		var n int
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			n = e.ExportN()
			assert.Positive(c, n)
		}, 2*time.Second, time.Microsecond, "blocked export not attempted")

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NoError(c, b.OnEmit(ctx, new(Record)))
		}, time.Second, time.Microsecond, "OnEmit blocked")

		e.ExportTrigger <- struct{}{}
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, e.ExportN(), n)
		}, 2*time.Second, time.Microsecond, "flush not retriggered")

		releaseExport()
		assert.NoError(t, b.Shutdown(ctx))
		assert.Equal(t, 3, e.ExportN())
	})

	t.Run("Shutdown", func(t *testing.T) {
		t.Run("Error", func(t *testing.T) {
			e := &testExporter{Err: assert.AnError}
			b := NewBatchProcessor(e)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
			assert.ErrorIs(t, b.Shutdown(ctx), assert.AnError, "exporter error not returned")
			assert.NoError(t, b.Shutdown(ctx))
		})

		t.Run("Multiple", func(t *testing.T) {
			e := &testExporter{}
			b := NewBatchProcessor(e)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()

			const shutdowns = 3
			for i := 0; i < shutdowns; i++ {
				assert.NoError(t, b.Shutdown(ctx))
			}
			assert.Equal(t, 1, e.ShutdownN(), "exporter Shutdown calls")
		})

		t.Run("OnEmit", func(t *testing.T) {
			e := &testExporter{}
			b := NewBatchProcessor(e)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
			assert.NoError(t, b.Shutdown(ctx))

			want := e.ExportN()
			assert.NoError(t, b.OnEmit(ctx, new(Record)))
			assert.Equal(t, want, e.ExportN(), "Export called after shutdown")
		})

		t.Run("ForceFlush", func(t *testing.T) {
			e := &testExporter{}
			b := NewBatchProcessor(e)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()

			assert.NoError(t, b.OnEmit(ctx, new(Record)))
			assert.NoError(t, b.Shutdown(ctx))

			want := e.ForceFlushN()
			assert.NoError(t, b.ForceFlush(ctx))
			assert.Equal(t, want, e.ForceFlushN(), "ForceFlush called after shutdown")
		})

		t.Run("CanceledContext", func(t *testing.T) {
			e := &testExporter{}
			e.ExportTrigger = make(chan struct{})
			t.Cleanup(func() { close(e.ExportTrigger) })
			b := NewBatchProcessor(e)

			ctx := context.Background()
			c, cancel := context.WithCancel(ctx)
			cancel()

			assert.ErrorIs(t, b.Shutdown(c), context.Canceled)
			require.Eventually(t, func() bool {
				select {
				case <-b.done:
					return true
				default:
					return false
				}
			}, 2*time.Second, time.Microsecond, "processor shutdown did not finish")
			assert.Equal(t, 1, e.ShutdownN(), "exporter Shutdown calls")
		})
	})

	t.Run("ForceFlush", func(t *testing.T) {
		t.Run("Flush", func(t *testing.T) {
			e := &testExporter{Err: assert.AnError}
			b := NewBatchProcessor(
				e,
				WithMaxQueueSize(100),
				WithExportMaxBatchSize(10),
				WithExportInterval(time.Hour),
				WithExportTimeout(time.Hour),
			)
			defer func() { assert.ErrorIs(t, b.Shutdown(ctx), assert.AnError) }()

			r := new(Record)
			r.SetBody(log.BoolValue(true))
			require.NoError(t, b.OnEmit(ctx, r))

			assert.ErrorIs(t, b.ForceFlush(ctx), assert.AnError, "exporter error not returned")
			assert.Equal(t, 1, e.ForceFlushN(), "exporter ForceFlush calls")
			if assert.Equal(t, 1, e.ExportN(), "exporter Export calls") {
				got := e.Records()
				if assert.Len(t, got[0], 1, "records received") {
					assert.Equal(t, *r, got[0][0])
				}
			}
		})

		t.Run("CanceledContext", func(t *testing.T) {
			e := &testExporter{}
			e.ExportTrigger = make(chan struct{})
			b := NewBatchProcessor(e)
			var release sync.Once
			releaseExport := func() { release.Do(func() { close(e.ExportTrigger) }) }
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
			defer releaseExport()

			r := new(Record)
			r.SetBody(log.BoolValue(true))
			_ = b.OnEmit(ctx, r)
			c, cancel := context.WithCancel(ctx)
			cancel()
			assert.ErrorIs(t, b.ForceFlush(c), context.Canceled)
		})
	})

	t.Run("CanceledWhileWaiting", func(t *testing.T) {
		blockedExporter := func() (*testExporter, <-chan struct{}, func()) {
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			e := &testExporter{
				ExportFunc: func(context.Context, []Record) error {
					close(started)
					<-release
					return nil
				},
			}
			return e, started, func() { once.Do(func() { close(release) }) }
		}

		t.Run("ForceFlush", func(t *testing.T) {
			exp, exportStarted, unblock := blockedExporter()
			b := NewBatchProcessor(
				exp,
				WithExportInterval(time.Hour),
			)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
			defer unblock()

			require.NoError(t, b.OnEmit(ctx, new(Record)))
			fCtx, cancel := context.WithCancel(ctx)
			flushErr := make(chan error, 1)
			go func() { flushErr <- b.ForceFlush(fCtx) }()

			<-exportStarted
			cancel()
			assert.ErrorIs(t, <-flushErr, context.Canceled)
		})

		t.Run("Shutdown", func(t *testing.T) {
			exp, exportStarted, unblock := blockedExporter()
			shutdownDone := make(chan struct{})
			exp.ShutdownFunc = func(context.Context) error {
				close(shutdownDone)
				return nil
			}
			b := NewBatchProcessor(
				exp,
				WithMaxQueueSize(1),
				WithExportInterval(time.Hour),
			)
			defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
			defer unblock()

			require.NoError(t, b.OnEmit(ctx, new(Record)))
			<-exportStarted

			sCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			assert.ErrorIs(t, b.Shutdown(sCtx), context.DeadlineExceeded)
			unblock()
			<-shutdownDone
		})
	})

	t.Run("DroppedLogs", func(t *testing.T) {
		orig := global.GetLogger()
		t.Cleanup(func() { global.SetLogger(orig) })
		buf := new(syncBuffer)
		stdr.SetVerbosity(1)
		global.SetLogger(stdr.New(stdlog.New(buf, "", 0)))

		e := &testExporter{}
		e.ExportTrigger = make(chan struct{})
		var release sync.Once
		releaseExport := func() { release.Do(func() { close(e.ExportTrigger) }) }

		b := NewBatchProcessor(
			e,
			WithMaxQueueSize(1),
			WithExportMaxBatchSize(1),
			WithExportInterval(time.Hour),
			WithExportTimeout(time.Hour),
		)
		defer func() { assert.NoError(t, b.Shutdown(ctx)) }()
		defer releaseExport()
		r := new(Record)
		// First record will be blocked by testExporter.Export
		assert.NoError(t, b.OnEmit(ctx, r), "exported record")
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Positive(c, e.ExportN())
		}, 2*time.Second, time.Microsecond, "blocked export not attempted")

		// The second record is queued while Export is blocked. The third
		// replaces it because the queue is full.
		assert.NoError(t, b.OnEmit(ctx, r), "first queued")
		assert.NoError(t, b.OnEmit(ctx, r), "second queued")

		releaseExport()

		wantMsg := `"level"=1 "msg"="dropped log records" "dropped"=1`
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Contains(c, buf.String(), wantMsg)
		}, 2*time.Second, time.Microsecond)

		_ = b.Shutdown(ctx)
	})
}

// TestBatchProcessorExporterBackpressureNoBusyLoop is a regression test for
// CVE-2026-81872. While the exporter is blocked and the queue holds more than
// a full batch, the processor must wait for the in-flight export instead of
// repeatedly re-triggering itself and re-copying queued records.
func TestBatchProcessorExporterBackpressureNoBusyLoop(t *testing.T) {
	const batch = 10

	exportStarted := make(chan struct{})
	releaseExport := make(chan struct{})
	var startOnce, release sync.Once
	e := &testExporter{
		ExportFunc: func(context.Context, []Record) error {
			startOnce.Do(func() { close(exportStarted) })
			<-releaseExport
			return nil
		},
	}
	b := NewBatchProcessor(
		e,
		WithMaxQueueSize(10*batch),
		WithExportMaxBatchSize(batch),
		WithExportInterval(time.Hour),
		WithExportTimeout(time.Hour),
	)
	unblock := func() { release.Do(func() { close(releaseExport) }) }
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()
	defer unblock()

	ctx := context.Background()
	// Fill the queue well beyond a single batch so a full batch is always
	// ready while the first export is blocked.
	var emitted int
	for i := 0; i < 10*batch; i++ {
		require.NoError(t, b.OnEmit(ctx, new(Record)))
		emitted++
	}
	<-exportStarted

	// Keep size-triggering OnEmit calls flowing while the exporter is
	// backpressured. Once the queue is full every one of them drops a record.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		require.NoError(t, b.OnEmit(ctx, new(Record)))
		emitted++
	}

	// The worker must stay parked in the single in-flight export: no further
	// export attempts, no further dequeues, and no polling of the queue. Each
	// polling pass consumes the dropped-records counter, so it must still hold
	// every record dropped since the export started.
	assert.Equal(t, 1, e.ExportN(), "exports attempted while exporter blocked")
	b.q.Lock()
	qLen := b.q.len
	b.q.Unlock()
	assert.Equal(t, 10*batch, qLen, "queue length")
	assert.Equal(
		t, uint64(emitted-batch-qLen), b.q.dropped.Load(),
		"queue polled while exporter blocked",
	)

	// Once the exporter makes progress, queued batches are exported.
	unblock()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, e.ExportN(), 1)
	}, 2*time.Second, time.Microsecond, "export not resumed after backpressure")
}

// TestBatchProcessorForceFlushBackpressureNoBusyLoop is a regression test for
// CVE-2026-81872. A ForceFlush issued while the exporter is blocked must wait
// for the worker rather than spinning and re-copying the queue until its
// context expires.
func TestBatchProcessorForceFlushBackpressureNoBusyLoop(t *testing.T) {
	const batch = 10

	exportStarted := make(chan struct{})
	releaseExport := make(chan struct{})
	var startOnce, release sync.Once
	e := &testExporter{
		ExportFunc: func(context.Context, []Record) error {
			startOnce.Do(func() { close(exportStarted) })
			<-releaseExport
			return nil
		},
	}
	b := NewBatchProcessor(
		e,
		WithMaxQueueSize(10*batch),
		WithExportMaxBatchSize(batch),
		WithExportInterval(time.Hour),
		WithExportTimeout(time.Hour),
	)
	unblock := func() { release.Do(func() { close(releaseExport) }) }
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()
	defer unblock()

	ctx := context.Background()
	for i := 0; i < 10*batch; i++ {
		require.NoError(t, b.OnEmit(ctx, new(Record)))
	}
	<-exportStarted

	// Hold the queue lock. A ForceFlush that polls the queue while the
	// exporter is backpressured blocks here instead of waiting for the
	// exporter goroutine and honoring its deadline.
	b.q.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(b.q.Unlock) }
	defer unlock()

	fCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	flushErr := make(chan error, 1)
	go func() { flushErr <- b.ForceFlush(fCtx) }()
	select {
	case err := <-flushErr:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		unlock()
		t.Fatal("ForceFlush polled the queue while the exporter was blocked")
	}
	unlock()

	assert.Equal(t, 1, e.ExportN(), "exports attempted while exporter blocked")
	assert.Zero(t, e.ForceFlushN(), "exporter ForceFlush called while blocked")
}

func TestBatchProcessorForceFlushExportError(t *testing.T) {
	e := &testExporter{}
	e.ExportFunc = func(context.Context, []Record) error {
		return assert.AnError
	}

	b := NewBatchProcessor(
		e,
		WithExportMaxBatchSize(10),
		WithExportInterval(time.Hour),
	)
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()

	require.NoError(t, b.OnEmit(context.Background(), new(Record)))
	err := b.ForceFlush(context.Background())
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, e.ExportN())
	assert.Equal(t, 1, e.ForceFlushN())
}

func TestBatchProcessorCanceledFlushRetainsQueue(t *testing.T) {
	e := &testExporter{}
	b := NewBatchProcessor(
		e,
		WithMaxQueueSize(2),
		WithExportMaxBatchSize(2),
		WithExportInterval(time.Hour),
	)
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()
	r := new(Record)
	r.SetBody(log.BoolValue(true))
	require.NoError(t, b.OnEmit(context.Background(), r))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, b.ForceFlush(ctx), context.Canceled)
	assert.Zero(t, e.ExportN(), "Export calls")
	assert.Zero(t, e.ForceFlushN(), "ForceFlush calls")

	require.NoError(t, b.ForceFlush(context.Background()))
	records := e.Records()
	require.Len(t, records, 1, "exported batches")
	require.Len(t, records[0], 1, "exported records")
	assert.Equal(t, *r, records[0][0])
}

func TestBatchProcessorShutdownExportsBeforeExporterShutdown(t *testing.T) {
	var calls []string
	e := &testExporter{}
	e.ExportFunc = func(context.Context, []Record) error {
		calls = append(calls, "export")
		return nil
	}
	e.ForceFlushFunc = func(context.Context) error {
		calls = append(calls, "force flush")
		return nil
	}
	e.ShutdownFunc = func(context.Context) error {
		calls = append(calls, "shutdown")
		return nil
	}

	b := NewBatchProcessor(
		e,
		WithExportInterval(time.Hour),
	)
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()
	require.NoError(t, b.OnEmit(context.Background(), new(Record)))
	require.NoError(t, b.Shutdown(context.Background()))

	assert.Equal(t, []string{"export", "shutdown"}, calls)
}

func TestBatchProcessorForceFlushErrorShutdownConcurrentSafe(t *testing.T) {
	exportStarted := make(chan struct{})
	releaseExport := make(chan struct{})
	var release sync.Once

	e := &testExporter{}
	e.ExportFunc = func(context.Context, []Record) error {
		close(exportStarted)
		<-releaseExport
		return assert.AnError
	}

	b := NewBatchProcessor(
		e,
		WithExportInterval(time.Hour),
	)
	unblock := func() { release.Do(func() { close(releaseExport) }) }
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()
	defer unblock()
	require.NoError(t, b.OnEmit(context.Background(), new(Record)))

	flushErr := make(chan error, 1)
	go func() { flushErr <- b.ForceFlush(context.Background()) }()
	<-exportStarted

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- b.Shutdown(context.Background()) }()
	unblock()

	assert.ErrorIs(t, <-flushErr, assert.AnError)
	assert.NoError(t, <-shutdownErr)
}

func TestBatchProcessorSerializesExporterCallsConcurrentSafe(t *testing.T) {
	// Track all exporter entry points with the same counters so an overlap
	// between any combination of calls raises maxActive above one.
	var (
		active    atomic.Int32
		maxActive atomic.Int32
	)
	call := func() {
		n := active.Add(1)
		for {
			old := maxActive.Load()
			if n <= old || maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		// Keep the callback active long enough for a concurrent call to enter if
		// the BatchProcessor stops serializing exporter calls.
		time.Sleep(time.Microsecond)
		active.Add(-1)
	}

	e := &testExporter{}
	e.ExportFunc = func(context.Context, []Record) error {
		call()
		return nil
	}
	e.ForceFlushFunc = func(context.Context) error {
		call()
		return nil
	}
	e.ShutdownFunc = func(context.Context) error {
		call()
		return nil
	}

	b := NewBatchProcessor(
		e,
		WithMaxQueueSize(64),
		WithExportMaxBatchSize(8),
		WithExportInterval(time.Millisecond),
	)
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()

	// Keep emission and flush traffic in flight so Shutdown races with work
	// already being handled instead of only exercising the stopped fast path.
	var wg sync.WaitGroup
	// Use a child context as the shared stop signal for the looping callers.
	// Canceling it also unblocks a pending ForceFlush if the test exits early.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = b.OnEmit(ctx, new(Record))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = b.ForceFlush(ctx)
			}
		}()
	}

	// Ensure the ordinary export and flush paths reached the exporter before
	// initiating Shutdown and testing serialization of the terminal call.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Positive(c, e.ExportN(), "Export calls")
		assert.Positive(c, e.ForceFlushN(), "ForceFlush calls")
	}, 10*time.Second, time.Microsecond, "exporter methods not exercised")
	require.NoError(t, b.Shutdown(context.Background()))
	cancel()
	wg.Wait()

	assert.Equal(t, int32(1), maxActive.Load(), "concurrent exporter calls")
	assert.Zero(t, active.Load(), "active exporter calls")
	assert.Equal(t, 1, e.ShutdownN(), "Shutdown calls")
}

func TestBatchProcessorConcurrentSafe(t *testing.T) {
	e := &testExporter{}
	b := NewBatchProcessor(e)
	defer func() { assert.NoError(t, b.Shutdown(context.Background())) }()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_ = b.OnEmit(ctx, new(Record))
			_ = b.ForceFlush(ctx)
			_ = b.Shutdown(ctx)
		}()
	}
	wg.Wait()
}

func TestQueue(t *testing.T) {
	var r Record
	r.SetBody(log.BoolValue(true))

	t.Run("newQueue", func(t *testing.T) {
		const size = 1
		q := newQueue(size)
		assert.Equal(t, 0, q.len)
		assert.Equal(t, size, q.cap, "capacity")
		assert.Equal(t, size, q.read.Len(), "read ring")
		assert.Same(t, q.read, q.write, "different rings")
	})

	t.Run("Enqueue", func(t *testing.T) {
		const size = 2
		q := newQueue(size)
		enqueue := func(r Record) int {
			n, accepted := q.Enqueue(r)
			assert.True(t, accepted)
			return n
		}

		var notR Record
		notR.SetBody(log.IntValue(10))

		assert.Equal(t, 1, enqueue(notR), "incomplete batch")
		assert.Equal(t, 1, q.len, "length")
		assert.Equal(t, size, q.cap, "capacity")

		assert.Equal(t, 2, enqueue(r), "complete batch")
		assert.Equal(t, 2, q.len, "length")
		assert.Equal(t, size, q.cap, "capacity")

		assert.Equal(t, 2, enqueue(r), "overflow batch")
		assert.Equal(t, 2, q.len, "length")
		assert.Equal(t, size, q.cap, "capacity")

		assert.Equal(t, []Record{r, r}, q.Flush(), "flushed Records")
	})

	t.Run("Dropped", func(t *testing.T) {
		q := newQueue(1)

		_, _ = q.Enqueue(r)
		_, _ = q.Enqueue(r)
		assert.Equal(t, uint64(1), q.Dropped(), "fist")

		_, _ = q.Enqueue(r)
		_, _ = q.Enqueue(r)
		assert.Equal(t, uint64(2), q.Dropped(), "second")
	})

	t.Run("Flush", func(t *testing.T) {
		const size = 2
		q := newQueue(size)
		q.write.Value = r
		q.write = q.write.Next()
		q.len = 1

		assert.Equal(t, []Record{r}, q.Flush(), "flushed")
	})

	t.Run("Dequeue", func(t *testing.T) {
		const size = 3
		q := newQueue(size)
		for i := 0; i < size-1; i++ {
			q.write.Value = r
			q.write = q.write.Next()
			q.len++
		}

		buf := make([]Record, 1)
		n, remaining := q.Dequeue(buf)
		assert.Equal(t, 1, n, "dequeued")
		assert.Equal(t, size-2, remaining, "remaining")
		assert.Equal(t, []Record{r}, buf, "records")
		assert.Equal(t, Record{}, q.read.Prev().Value, "retained record")

		buf = make([]Record, size)
		n, remaining = q.Dequeue(buf)
		assert.Equal(t, 1, n, "dequeued")
		assert.Zero(t, remaining, "remaining")
		assert.Equal(t, []Record{r}, buf[:n], "records")
	})

	t.Run("Close", func(t *testing.T) {
		q := newQueue(1)
		_, accepted := q.Enqueue(r)
		require.True(t, accepted)

		q.Close()
		assert.Equal(t, []Record{r}, q.Flush())
		_, accepted = q.Enqueue(r)
		assert.False(t, accepted)
		assert.Empty(t, q.Flush())
	})

	t.Run("ConcurrentSafe", func(t *testing.T) {
		const goRoutines = 10

		flushed := make(chan []Record, goRoutines)
		out := make([]Record, 0, goRoutines)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for recs := range flushed {
				out = append(out, recs...)
			}
		}()

		var wg sync.WaitGroup
		wg.Add(goRoutines)

		b := newQueue(goRoutines)
		for i := 0; i < goRoutines; i++ {
			go func() {
				defer wg.Done()
				_, _ = b.Enqueue(Record{})
				flushed <- b.Flush()
			}()
		}

		wg.Wait()
		close(flushed)
		<-done

		assert.Len(t, out, goRoutines, "flushed Records")
	})
}

func TestQueueCloseConcurrentSafe(t *testing.T) {
	const writers = 32
	q := newQueue(writers)
	start := make(chan struct{})
	accepted := make(chan int64, writers)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var r Record
			r.SetBody(log.IntValue(i))
			if _, ok := q.Enqueue(r); ok {
				accepted <- int64(i)
			}
		}(i)
	}
	flushed := make(chan []Record, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		q.Close()
		flushed <- q.Flush()
	}()

	close(start)
	wg.Wait()
	close(accepted)

	var want []int64
	for id := range accepted {
		want = append(want, id)
	}
	var got []int64
	for _, r := range <-flushed {
		got = append(got, r.Body().AsInt64())
	}
	assert.ElementsMatch(t, want, got)
	assert.Zero(t, q.len)
	_, ok := q.Enqueue(Record{})
	assert.False(t, ok)
}

type blockingExporter struct {
	release chan struct{}
}

func (e *blockingExporter) Export(ctx context.Context, _ []Record) error {
	select {
	case <-e.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*blockingExporter) ForceFlush(context.Context) error { return nil }

func (*blockingExporter) Shutdown(context.Context) error { return nil }

func BenchmarkBatchProcessorOnEmitExporterBlocked(b *testing.B) {
	exp := &blockingExporter{
		release: make(chan struct{}),
	}
	bp := NewBatchProcessor(
		exp,
		WithExportInterval(time.Hour),
		WithExportTimeout(time.Hour),
	)
	ctx := context.Background()
	defer func() { assert.NoError(b, bp.Shutdown(ctx)) }()
	defer close(exp.release)

	r := new(Record)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bp.OnEmit(ctx, r)
		}
	})
}

func BenchmarkBatchProcessorForceFlush(b *testing.B) {
	bp := NewBatchProcessor(noopExporter{}, WithExportInterval(time.Hour))
	ctx := context.Background()
	defer func() { assert.NoError(b, bp.Shutdown(ctx)) }()

	r := new(Record)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_ = bp.OnEmit(ctx, r)
		b.StartTimer()
		_ = bp.ForceFlush(ctx)
	}
}
