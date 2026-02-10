// profiling.go
//
// Optional profiling and execution-tracing support for HistoryScanner.
//
// When enabled via the WithProfiling ScannerOption, this file starts an HTTP
// server that exposes the standard net/http/pprof endpoints and, optionally,
// an execution trace that is written to disk for the duration of the scan.
// Neither capability affects scanning correctness; if anything in this file
// fails the scanner can still operate normally.
package objstore

import (
	"context"
	"fmt"
	"net/http"
	pprof "net/http/pprof"
	"os"
	"runtime/trace"
	"time"
)

// ProfilingConfig specifies profiling options for the scanner.
//
// When provided to NewHistoryScanner via WithProfiling option, it starts
// an HTTP server with pprof endpoints for on-demand profiling.
type ProfilingConfig struct {
	// EnableProfiling starts an HTTP server with pprof endpoints.
	// When true, users can capture profiles using curl or go tool pprof.
	EnableProfiling bool

	// ProfileAddr specifies the address for the profiling HTTP server.
	// Defaults to ":6060" if empty.
	// Use "localhost:6060" to restrict to local access.
	ProfileAddr string

	// Trace enables execution tracing for the duration of the scan.
	// The trace is written to TraceOutputPath.
	Trace bool

	// TraceOutputPath specifies where to write the execution trace.
	// Defaults to "./trace.out" if empty and Trace is true.
	TraceOutputPath string
}

// ScannerOption is a function that configures a HistoryScanner during
// construction. Options are applied in the order they are passed to
// NewHistoryScanner; later options therefore override earlier ones when they
// touch the same field.
type ScannerOption func(*HistoryScanner)

// WithProfiling returns a ScannerOption that enables profiling with the given configuration.
//
// Example:
//
//	scanner, err := NewHistoryScanner(gitDir,
//	    WithProfiling(&ProfilingConfig{
//	        EnableProfiling: true,
//	        ProfileAddr:     ":6060",
//	        Trace:           true,
//	        TraceOutputPath: "./trace.out",
//	    }),
//	)
func WithProfiling(config *ProfilingConfig) ScannerOption {
	return func(hs *HistoryScanner) {
		if config == nil {
			return
		}

		// Apply default values when not explicitly configured.
		if config.EnableProfiling && config.ProfileAddr == "" {
			config.ProfileAddr = ":6060"
		}
		if config.Trace && config.TraceOutputPath == "" {
			config.TraceOutputPath = "./trace.out"
		}

		hs.profiling = config
	}
}

// startProfiling starts the HTTP profiling server and/or execution trace
// based on the current ProfilingConfig.
//
// Error semantics: a non-nil error is returned only when trace file creation
// or trace.Start fails -- conditions that indicate a real I/O problem the
// caller should surface. HTTP server bind failures are logged to stderr but
// do not produce an error because profiling is advisory and should never
// block scanning.
func (hs *HistoryScanner) startProfiling() error {
	if hs.profiling == nil {
		return nil
	}

	if hs.profiling.EnableProfiling {
		mux := http.NewServeMux()
		// Register pprof handlers explicitly to avoid dependency on the default mux.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		hs.profileServer = &http.Server{
			Addr:    hs.profiling.ProfileAddr,
			Handler: mux,
		}

		go func() {
			if err := hs.profileServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// Log error but don't fail the scan.
				fmt.Fprintf(os.Stderr, "Profiling server error: %v\n", err)
			}
		}()

		// Brief pause to give the goroutine time to bind the listening socket.
		// This is a known limitation: there is no synchronization channel
		// back from ListenAndServe, so we rely on a short sleep. In practice
		// 100 ms is more than sufficient for a local TCP bind.
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintf(os.Stderr, "Profiling server started on %s\n", hs.profiling.ProfileAddr)
		fmt.Fprintf(os.Stderr, "Capture profiles with: curl http://%s/debug/pprof/heap > heap.prof\n", hs.profiling.ProfileAddr)
	}

	if hs.profiling.Trace {
		f, err := os.Create(hs.profiling.TraceOutputPath)
		if err != nil {
			return fmt.Errorf("create trace file: %w", err)
		}
		hs.traceFile = f
		if err := trace.Start(f); err != nil {
			f.Close()
			hs.traceFile = nil
			return fmt.Errorf("start trace: %w", err)
		}
	}

	return nil
}

// stopProfiling stops the HTTP profiling server and/or execution trace.
//
// Idempotency: stopProfiling is safe to call multiple times or when
// startProfiling was never called. Each resource (HTTP server, trace file)
// is nil-checked and cleared after shutdown, so a second call is a no-op.
func (hs *HistoryScanner) stopProfiling() {
	if hs.profiling == nil {
		return
	}

	if hs.profileServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := hs.profileServer.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error shutting down profiling server: %v\n", err)
		}
		hs.profileServer = nil
	}

	if hs.traceFile != nil {
		trace.Stop()
		hs.traceFile.Close()
		hs.traceFile = nil
	}
}
