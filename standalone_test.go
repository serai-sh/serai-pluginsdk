// SPDX-License-Identifier: Apache-2.0

package pluginsdk_test

// These tests run the SDK against a fake gateway that is the plugin API and
// nothing else, so they run wherever the SDK does: they are published with
// it, and import only what it is published with. In serai's own tree the
// SDK also runs against the real gateway (pluginsdk_test.go,
// source_test.go).

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/serai-sh/serai-pluginsdk"
	pluginv1 "github.com/serai-sh/serai-pluginsdk/gen/go/serai/plugin/v1"
	"github.com/serai-sh/serai-pluginsdk/internal/pluginwire"
)

// fakeGateway answers a plugin as a gateway does: its config, OK to every
// registration, one Task on any Host, and a record of what was sent.
type fakeGateway struct {
	conn       *pluginwire.Conn
	registered chan string
	sent       chan *pluginv1.SendToTask
	cards      chan *pluginv1.Card
}

func startFakeGateway(t *testing.T, rwc net.Conn) *fakeGateway {
	t.Helper()
	g := &fakeGateway{registered: make(chan string, 8), sent: make(chan *pluginv1.SendToTask, 8), cards: make(chan *pluginv1.Card, 8)}
	g.conn = pluginwire.New(rwc, pluginwire.Options{
		Hello: &pluginv1.Hello{Name: "hello", Version: "0.1.0", SeraiVersion: "fake 1.2.3", Scopes: []string{"commands", "read", "send"}},
		OnRequest: func(_ context.Context, req *pluginv1.Request) *pluginv1.Response {
			switch c := req.Call.(type) {
			case *pluginv1.Request_GetConfig:
				return &pluginv1.Response{Result: &pluginv1.Response_Config{Config: &pluginv1.Config{Values: map[string]string{"greeting": "Hi"}}}}
			case *pluginv1.Request_RegisterCommand:
				g.registered <- c.RegisterCommand.GetCommand().GetNoun() + " " + c.RegisterCommand.GetCommand().GetVerb()
				return pluginwire.OK()
			case *pluginv1.Request_ListTasks:
				return &pluginv1.Response{Result: &pluginv1.Response_TaskList{TaskList: &pluginv1.TaskList{Tasks: []*pluginv1.Task{
					{Id: "t1", Host: c.ListTasks.GetHost(), Name: "api", Status: pluginv1.TaskStatus_TASK_STATUS_ARRIVED},
				}}}}
			case *pluginv1.Request_SendToTask:
				g.sent <- c.SendToTask
				return pluginwire.OK()
			case *pluginv1.Request_PublishCard:
				g.cards <- c.PublishCard.GetCard()
				return pluginwire.OK()
			}
			return nil
		},
	})
	t.Cleanup(func() { g.conn.Close() })
	return g
}

func TestServeAgainstTheAPI(t *testing.T) {
	a, b := net.Pipe()
	g := startFakeGateway(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type read struct {
		task *pluginv1.Task
		err  error
	}
	reads := make(chan read, 1)
	served := make(chan error, 1)
	go func() {
		served <- pluginsdk.Serve(ctx, a, "hello", func(p *pluginsdk.Plugin) error {
			if got := p.SeraiVersion(); got != "fake 1.2.3" {
				t.Errorf("SeraiVersion = %q, want the gateway's Hello's", got)
			}
			if got := p.Scopes(); len(got) != 3 {
				t.Errorf("Scopes = %q", got)
			}
			if err := p.Command(pluginsdk.Command{Noun: "hello", Verb: "greet", Effect: pluginsdk.Read}, func(_ context.Context, inv pluginsdk.Invocation) (string, error) {
				return p.Config("greeting") + ", " + inv.Flags["name"] + " (" + inv.Caller + ")", nil
			}); err != nil {
				return err
			}
			if err := p.Command(pluginsdk.Command{Noun: "hello", Verb: "no"}, func(context.Context, pluginsdk.Invocation) (string, error) {
				return "", pluginsdk.Refused("not today")
			}); err != nil {
				return err
			}
			ts, err := p.ListTasks(ctx, &pluginv1.ListTasks{Host: "devbox"})
			var task *pluginv1.Task
			if len(ts) == 1 {
				task = ts[0]
			}
			reads <- read{task, err}
			return p.SendToTask(ctx, &pluginv1.SendToTask{Host: "devbox", TaskId: "t1", Text: "look"})
		})
	}()

	for _, want := range []string{"hello greet", "hello no"} {
		select {
		case got := <-g.registered:
			if got != want {
				t.Fatalf("registered %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("the plugin never registered %q", want)
		}
	}
	r := <-reads
	if r.err != nil || r.task.GetHost() != "devbox" || pluginsdk.StatusWord(r.task.GetStatus()) != pluginsdk.StatusArrived {
		t.Fatalf("ListTasks: %v, %v", r.task, r.err)
	}
	if s := <-g.sent; s.GetTaskId() != "t1" || s.GetText() != "look" {
		t.Errorf("sent %v", s)
	}

	resp, err := g.conn.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_InvokeCommand{InvokeCommand: &pluginv1.InvokeCommand{
		Noun: "hello", Verb: "greet", Flags: map[string]string{"name": "you"}, Caller: "cli",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetCommandOutput().GetText(); got != "Hi, you (cli)" {
		t.Errorf("hello greet printed %q", got)
	}
	_, err = g.conn.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_InvokeCommand{InvokeCommand: &pluginv1.InvokeCommand{Noun: "hello", Verb: "no"}}})
	if pluginsdk.CodeOf(err) != pluginv1.Code_CODE_REFUSED {
		t.Errorf("hello no: %v, want refused", err)
	}

	g.conn.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve ended with %v once the gateway closed, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return when the gateway closed")
	}
}

// A Card's covers, badges and details reach the API as they were given, each
// SDK tone as the API's of the same name.
func TestPublishCardCarriesCoversBadgesAndDetails(t *testing.T) {
	a, b := net.Pipe()
	g := startFakeGateway(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tones := []pluginsdk.Tone{pluginsdk.ToneNeutral, pluginsdk.ToneGood, pluginsdk.ToneInfo, pluginsdk.ToneWarn, pluginsdk.ToneBad}
	go pluginsdk.Serve(ctx, a, "hello", func(p *pluginsdk.Plugin) error {
		c := pluginsdk.Card{ID: "pr", Host: "devbox", TaskID: "t1", Title: "#1", Markdown: "sum", Details: "more", Covers: []string{"a/b#1"}}
		for _, tone := range tones {
			c.Badges = append(c.Badges, pluginsdk.Badge{Label: "x", Tone: tone})
		}
		return p.PublishCard(ctx, c)
	})
	select {
	case c := <-g.cards:
		if c.GetDetails() != "more" || len(c.GetCovers()) != 1 || c.GetCovers()[0] != "a/b#1" || len(c.GetBadges()) != len(tones) {
			t.Fatalf("card = %v", c)
		}
		want := []string{"TONE_NEUTRAL", "TONE_GOOD", "TONE_INFO", "TONE_WARN", "TONE_BAD"}
		for i, b := range c.GetBadges() {
			if b.GetTone().String() != want[i] {
				t.Errorf("badge %d is %s, want %s", i, b.GetTone(), want[i])
			}
		}
	case <-ctx.Done():
		t.Fatal("the plugin never published its Card")
	}
}

// A status event reaches OnEvent with its Task and its statuses in the SDK's
// words.
func TestEventsReachOnEvent(t *testing.T) {
	a, b := net.Pipe()
	g := startFakeGateway(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan pluginsdk.Event, 1)
	ready := make(chan struct{})
	go func() {
		_ = pluginsdk.Serve(ctx, a, "hello", func(p *pluginsdk.Plugin) error {
			p.OnEvent(func(_ context.Context, ev pluginsdk.Event) { got <- ev })
			close(ready)
			return nil
		})
	}()
	<-ready
	if err := g.conn.Notify(ctx, &pluginv1.Event{JournalEvent: &pluginv1.JournalEvent{Kind: "turn_finished", Host: "devbox", TaskId: "t1",
		TaskName: "api", Project: "web", From: pluginv1.TaskStatus_TASK_STATUS_UNDERWAY, To: pluginv1.TaskStatus_TASK_STATUS_ARRIVED}}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-got:
		task, project, from, to := ev.Status()
		if task != "api" || project != "web" || from != pluginsdk.StatusUnderway || to != pluginsdk.StatusArrived || ev.Kind != "turn_finished" {
			t.Errorf("event = %+v", ev)
		}
	case <-ctx.Done():
		t.Fatal("the event never reached OnEvent")
	}
}
