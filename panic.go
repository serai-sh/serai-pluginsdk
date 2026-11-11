// SPDX-License-Identifier: Apache-2.0

package pluginsdk

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
)

// A built-in plugin runs in the gateway's process, where an unrecovered
// panic on any goroutine ends the gateway. So every goroutine the SDK runs
// plugin code on recovers. A panic in an event handler or in Go is logged
// with its stack and ends Serve with an error, and the gateway's supervisor
// restarts the plugin after its backoff, as it would a plugin process that
// crashed. A command handler's panic fails that one call (the connection
// recovers it, as it does for every request) and the plugin keeps running:
// restarting would drop the answer the caller is waiting for. A goroutine
// the plugin starts with a plain `go` statement has none of this; start
// them with Go.

// Go runs fn on a goroutine the plugin owns. ctx ends when the plugin
// stops, and Serve waits for fn to return, so fn must return once ctx is
// done. A panic in fn is recovered and restarts the plugin instead of
// crashing the process.
func (p *Plugin) Go(fn func(ctx context.Context)) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				p.crash("a goroutine", r)
			}
		}()
		fn(p.ctx)
	}()
}

func (p *Plugin) runEvent(ctx context.Context, fn func(context.Context, Event), ev Event) {
	defer func() {
		if r := recover(); r != nil {
			p.crash("an event handler ("+ev.Kind+")", r)
		}
	}()
	fn(ctx, ev)
}

// crash logs a recovered panic and ends Serve with it. Only the first
// counts: the plugin is on its way out already.
func (p *Plugin) crash(where string, r any) {
	log.Printf("plugin: panic in %s: %v\n%s", where, r, debug.Stack())
	select {
	case p.crashed <- fmt.Errorf("panicked in %s: %v", where, r):
	default:
	}
	p.cancel()
}
