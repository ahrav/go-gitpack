// Package objstore provides profiling support for performance analysis.
//
// This file implements optional profiling capabilities using Go's standard
// net/http/pprof package, allowing on-demand profile capture via HTTP endpoints.
package objstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof" // Register pprof handlers
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

// ScannerOption is a function that configures a HistoryScanner during construction.
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

// startProfiling starts the HTTP profiling server and/or trace based on configuration.
// Returns an error if profiling setup fails, but scanning can continue.
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

		// Brief pause ensures the server is ready before continuing.
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

// stopProfiling stops the HTTP profiling server and/or trace.
// It ensures graceful shutdown even if called during error handling.
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
