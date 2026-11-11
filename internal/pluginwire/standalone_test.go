// SPDX-License-Identifier: Apache-2.0

package pluginwire

// Published with the SDK: these tests import nothing it isn't published
// with.

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/serai-sh/serai-pluginsdk/gen/go/serai/plugin/v1"
)

const wait = 5 * time.Second

func pair(t *testing.T, a, b Options) (*Conn, *Conn) {
	t.Helper()
	x, y := net.Pipe()
	ca, cb := New(x, a), New(y, b)
	t.Cleanup(func() { ca.Close(); cb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := ca.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cb.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	return ca, cb
}

// Both sides call each other at once over one synchronous pipe: neither
// side's reading waits on its writing.
func TestCallsBothWays(t *testing.T) {
	echo := func(_ context.Context, req *pluginv1.Request) *pluginv1.Response {
		return &pluginv1.Response{Result: &pluginv1.Response_CommandOutput{CommandOutput: &pluginv1.CommandOutput{Text: req.GetInvokeCommand().GetVerb()}}}
	}
	a, b := pair(t, Options{Hello: &pluginv1.Hello{Name: "a"}, OnRequest: echo}, Options{Hello: &pluginv1.Hello{Name: "b"}, OnRequest: echo})
	if a.Peer().GetName() != "b" || a.Peer().GetApi() != API {
		t.Errorf("a's peer = %v", a.Peer())
	}
	errs := make(chan error, 200)
	for i := 0; i < 100; i++ {
		for _, c := range []*Conn{a, b} {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), wait)
				defer cancel()
				resp, err := c.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_InvokeCommand{InvokeCommand: &pluginv1.InvokeCommand{Verb: "v"}}})
				if err == nil && resp.GetCommandOutput().GetText() != "v" {
					err = Errorf(pluginv1.Code_CODE_INTERNAL, "got %v", resp)
				}
				errs <- err
			}()
		}
	}
	for i := 0; i < 200; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// A refusal comes back as an *Error with its code and words; an unanswered
// call as UNIMPLEMENTED; a handler's panic as INTERNAL, the connection kept.
func TestStatuses(t *testing.T) {
	a, _ := pair(t, Options{}, Options{OnRequest: func(_ context.Context, req *pluginv1.Request) *pluginv1.Response {
		switch req.Call.(type) {
		case *pluginv1.Request_GetConfig:
			return Fail(Errorf(pluginv1.Code_CODE_REFUSED, "not yours"))
		case *pluginv1.Request_ListHosts:
			panic("boom")
		}
		return nil
	}})
	ctx := context.Background()
	_, err := a.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_GetConfig{GetConfig: &pluginv1.GetConfig{}}})
	if CodeOf(err) != pluginv1.Code_CODE_REFUSED || !strings.Contains(err.Error(), "not yours") {
		t.Errorf("refused: %v", err)
	}
	if _, err := a.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListTasks{ListTasks: &pluginv1.ListTasks{}}}); CodeOf(err) != pluginv1.Code_CODE_UNIMPLEMENTED {
		t.Errorf("unanswered: %v", err)
	}
	if _, err := a.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListHosts{ListHosts: &pluginv1.ListHosts{}}}); CodeOf(err) != pluginv1.Code_CODE_INTERNAL {
		t.Errorf("panicked: %v", err)
	}
	if _, err := a.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_GetConfig{GetConfig: &pluginv1.GetConfig{}}}); CodeOf(err) != pluginv1.Code_CODE_REFUSED {
		t.Errorf("after the panic: %v", err)
	}
}

// A call given up on cancels the handler on the other side.
func TestCancelReachesTheHandler(t *testing.T) {
	cancelled := make(chan struct{})
	a, _ := pair(t, Options{}, Options{OnRequest: func(ctx context.Context, _ *pluginv1.Request) *pluginv1.Response {
		<-ctx.Done()
		close(cancelled)
		return OK()
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_GetConfig{GetConfig: &pluginv1.GetConfig{}}}); CodeOf(err) != pluginv1.Code_CODE_DEADLINE {
		t.Errorf("Call = %v, want deadline", err)
	}
	select {
	case <-cancelled:
	case <-time.After(wait):
		t.Fatal("the handler's context never ended")
	}
}

// Events arrive in order; a peer that speaks another API is refused at its
// Hello.
func TestEventsAndAPIMismatch(t *testing.T) {
	got := make(chan string, 3)
	a, _ := pair(t, Options{}, Options{OnEvent: func(ev *pluginv1.Event) { got <- ev.GetJournalEvent().GetKind() }})
	for _, k := range []string{"one", "two", "three"} {
		if err := a.Notify(context.Background(), &pluginv1.Event{JournalEvent: &pluginv1.JournalEvent{Kind: k}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"one", "two", "three"} {
		if k := <-got; k != want {
			t.Fatalf("event %q, want %q", k, want)
		}
	}

	x, y := net.Pipe()
	c := New(x, Options{})
	defer c.Close()
	go func() {
		_ = WriteFrame(y, &pluginv1.Frame{Body: &pluginv1.Frame_Hello{Hello: &pluginv1.Hello{Api: "serai.plugin.v9"}}})
	}()
	go func() { _, _ = ReadRaw(y) }()
	select {
	case <-c.Done():
		if !strings.Contains(c.Err().Error(), "serai.plugin.v9") {
			t.Errorf("closed with %v", c.Err())
		}
	case <-time.After(wait):
		t.Fatal("a peer on another API was not refused")
	}
}
