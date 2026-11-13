// SPDX-License-Identifier: Apache-2.0

// Package pluginsdk writes a serai plugin in a few lines of Go.
//
// A plugin is a program serai's gateway runs, speaking the plugin API
// (serai.plugin.v1) over its stdin and stdout, under the scopes its user
// accepted. Main connects, fetches the plugin's config, calls setup to
// register what the plugin contributes, and serves until the gateway closes
// the connection:
//
//	func main() {
//		pluginsdk.Main(func(p *pluginsdk.Plugin) error {
//			return p.Command(pluginsdk.Command{Noun: "hello", Verb: "greet", Effect: pluginsdk.Read},
//				func(ctx context.Context, inv pluginsdk.Invocation) (string, error) {
//					return "Hello, " + inv.Flags["name"] + "!", nil
//				})
//		})
//	}
//
// The reads (ListHosts, ListTasks, ReadConversation, GitStatus,
// ListPullRequests, ListEvents) and SendToTask take and give the API's own messages (gen/go/serai/plugin/v1),
// and Call sends any v1 request the SDK doesn't wrap. The gateway holds
// every call to the plugin's scopes.
//
// The package is published on its own as github.com/serai-sh/serai-pluginsdk,
// with the API's types and protocol and an example plugin; its README
// covers the manifest, the scopes and how a plugin is run.
package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	pluginv1 "github.com/serai-sh/serai-pluginsdk/gen/go/serai/plugin/v1"
	"github.com/serai-sh/serai-pluginsdk/internal/pluginwire"
)

// helloTimeout bounds waiting for the gateway's Hello; the gateway drops a
// plugin that hasn't said its own within 10 s.
const helloTimeout = 5 * time.Second

// callTimeout bounds each registration call setup makes.
const callTimeout = 10 * time.Second

// eventBuffer is how many events wait for OnEvent handlers.
const eventBuffer = 256

// Plugin is one connection to the gateway.
type Plugin struct {
	c      *pluginwire.Conn
	config map[string]string

	mu       sync.Mutex
	handlers map[string]Handler // "noun verb"
	source   SourceHandler
	onEvent  []func(context.Context, Event)
	events   chan Event

	// ctx ends when Serve returns; the goroutines the plugin runs (Go,
	// the event dispatch) end with it and Serve waits for them.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// crashed carries the first panic recovered on one of those
	// goroutines, which ends Serve with it (panic.go).
	crashed chan error
}

// Effect says what a command may do, and so where it appears: Read, Write
// and Confirm commands are MCP tools too (a Write call is audited, a
// Confirm one needs the user each time); Local ones are for the TUI and
// the CLI only.
type Effect int

const (
	Confirm Effect = iota // the zero value: never runs freely by mistake
	Read
	Write
	Local
)

// Command is a command the plugin adds: `serai <Noun> <Verb>`.
type Command struct {
	Noun, Verb string
	// Usage names the positional arguments: "<name>" required, "[name]"
	// optional, "<files>..." any number.
	Usage  string
	Short  string // one line of help
	Flags  []Flag
	Effect Effect
}

// Flag is one --flag. Type is "string" (default), "bool" or "int".
type Flag struct {
	Name, Type, Usage, Default string
	Required                   bool
}

// Invocation is one run of a command.
type Invocation struct {
	Args []string
	// Flags holds every flag's value, defaults included, as strings.
	Flags map[string]string
	// Caller is who ran it: "cli", "tui", "app", "mcp".
	Caller string
}

// Handler runs a command. Its output is printed as it is; an error fails
// the command with its message. Return Refused(...) to say no rather than
// fail.
type Handler func(ctx context.Context, inv Invocation) (string, error)

// Card is Markdown shown beside a Task (Host and TaskID), a Host (Host),
// or everywhere (neither). Publishing an ID again replaces the Card.
type Card struct {
	ID, Host, TaskID, Title, Markdown string
	Actions                           []Action
	// Covers names the pull requests the Card speaks for, "owner/repo#123":
	// beside its Task, serai shows no pull request card of its own for them
	// while the Card is there.
	Covers []string
	// Badges sit by the title, coloured by the frontend's theme from Tone.
	Badges []Badge
	// Details is Markdown folded behind "more". With it set, Markdown is
	// the summary and Actions fold away with the details.
	Details string
}

// Badge is a short label by a Card's title, "A" or "#3 in queue".
type Badge struct {
	Label string
	Tone  Tone
}

// Tone is what a badge says; the frontend colours it from its theme. Its
// values are serai.plugin.v1's Tone.
type Tone int

const (
	ToneNeutral Tone = iota
	ToneGood
	ToneInfo
	ToneWarn
	ToneBad
)

// Action is a Card button running one of this plugin's own commands, as
// the TUI's : prompt would: "hello greet --name you".
type Action struct{ Label, Command string }

// Source is the plugin's Tasks from outside serai: shown read-only in the
// sidebar and the fleet beside serai's own, with Label where a Task names
// its Host and "Open in <Label>" in place of its pane. Note is one line
// about the source as a whole (why it shows nothing, say).
type Source struct {
	Label, Note string
	Tasks       []ExternalTask
	// Groups are the source's Tasks that belong together (a plan, say),
	// drawn in serai's Groups section with each member once, under its
	// Group. A Task joins one by ExternalTask.Group.
	Groups []Group
	// Icon is the source's mark where serai names it, drawn about 14 px
	// square as an image (so an SVG never takes the theme's colours), and
	// IconDark in its place on a dark theme; nil for serai's own mark.
	// Symbol stands for it in a terminal: one character one cell wide, ""
	// for serai's "⧉".
	Icon, IconDark *Icon
	Symbol         string
}

// Icon is an SVG ("image/svg+xml") or a PNG ("image/png") of at most 16
// KiB. An SVG's root is <svg xmlns="http://www.w3.org/2000/svg">.
type Icon struct {
	MediaType string
	Data      []byte
}

func icon(i *Icon) *pluginv1.Icon {
	if i == nil {
		return nil
	}
	return &pluginv1.Icon{MediaType: i.MediaType, Data: i.Data}
}

// Group is one Group a Source shows. Lead is a member's ID, or "" for none;
// URL is where "Open in <Label>" goes, "" for none. Summary, when set, is
// drawn in place of serai's status roll-up; serai always adds its own
// at-the-gate count.
//
// Items are the Group's own checklist (a Factory plan's work items), called
// ItemsWord ("work items"): drawn on the Group's page and in its members'
// Context, never on a member's row, whose plan bar is the Task's own Plans
// (a session's task list). With Items, serai counts Summary's Done and
// Total from them, leaving cancelled ones out. At most 50.
type Group struct {
	ID, Title, Lead, URL string
	Summary              *GroupSummary
	Items                []PlanItem
	ItemsWord            string
}

// GroupSummary is the source's overview of a Group, in fields serai draws in
// its own type: a progress line when Total > 0 ("6 of 7 merged" with
// DoneWord "merged"), a meter as well with Bar, a neutral State word
// ("blocked"), a Subtitle and a Link. Attention counts the members that
// need a person before they can go on, named by AttentionWord ("blocked"),
// drawn in amber.
type GroupSummary struct {
	Subtitle, DoneWord, State string
	Done, Total               int
	Bar                       bool
	Link                      *Link
	Attention                 int
	AttentionWord             string
}

// ExternalTask is one Task a Source shows. Status is one of StatusStabled,
// StatusUnderway, StatusAtTheGate and StatusArrived; Project is a Project's
// name, listed with serai's Projects of that name; URL is where "Open in
// <Label>" goes.
//
// ConversationRev, when set, says the source serves the Task's
// conversation (SourceHandler.Conversation); change it whenever that
// changes, and a frontend showing it reads it again. AcceptsMessages offers
// the composer (SourceHandler.Send), MessagesWhileWorking also while the
// Task is underway, Stoppable a Stop while it is (SourceHandler.Stop), and
// Gate the person's answer at the gate (SourceHandler.Answer). Archivable
// offers "Archive in <Label>" in the Task's menu (SourceHandler.Archive),
// which closes it for everyone in it, and Leavable "Leave"
// (SourceHandler.Leave), which takes it off the person's own lists.
type ExternalTask struct {
	ID, Name, Project, Status, URL, Preview string
	LastActivity                            time.Time
	Links                                   []Link
	ConversationRev                         string
	AcceptsMessages                         bool
	MessagesWhileWorking                    bool
	Stoppable                               bool
	Archivable                              bool
	Leavable                                bool
	Gate                                    *Gate
	// Group is the ID of the Source's Group the Task is in, "" for none.
	Group string

	// What the source knows beyond the status, each optional: who is in it,
	// its model, context and cost, a line on where it stands, and its work.
	// Plans are the Task's own (a session's task list), drawn as its row's
	// plan bar; a Group's checklist goes on the Group (Group.Items).
	Participants           []string
	Model, Cost, StateNote string
	ContextPercent         int
	ContextTokens          int64
	Plans                  []Plan
	PullRequests           []PullRequest
	Artifacts              []Link
	Previews               []Preview
}

// Gate is the tool call a Task at the gate waits on: Tool and its Input as
// the source shows them, what the person may answer, and in WhyNot why they
// may not approve when they may not.
type Gate struct {
	ID, Tool, Input, WhyNot string
	MayApprove, MayDeny     bool
	// Body, when set, is the call laid out as a card, drawn in place of the
	// Input: for a tool whose input is a document, such as a proposed plan.
	Body *GateBody
}

// GateBody is a gate's ask as a card: Action is what approving does
// ("Create plan"), Title what it is about, Count a line such as "4 work
// items"; then Sections of Markdown, a Folded one shortened behind "Show
// all", and numbered Items that open on their Detail.
type GateBody struct {
	Action, Title, Count string
	Sections             []GateSection
	Items                []GateItem
}

type GateSection struct {
	Heading, Markdown string
	Folded            bool
}

// GateItem is one numbered row; Kind is one word ("feature", "bug").
type GateItem struct {
	Label, Kind, Detail string
}

// Plan is a checklist the source keeps for a Task; Done items count toward
// its progress.
type Plan struct {
	Title, URL string
	Items      []PlanItem
}

// PlanItem is one item of a Plan: Kind its one word ("feature"), Since when
// it reached State, and Note a line on it, a warning with Attention (the
// item needs a person before it can go on: "Its dependency hasn't merged").
// An item whose State is "cancelled" stays listed, struck through, and
// counts for neither done nor total, so a GroupSummary that counts the
// same way agrees with its members' plan bars.
type PlanItem struct {
	Label, State, URL string
	Done              bool
	Kind, Note        string
	Since             time.Time
	Attention         bool
}

// PullRequest is a pull request a Task opened or follows, as the source
// reads it: Grade is a reviewer's ("A"–"F"), Notes short words ("queued").
type PullRequest struct {
	URL, Repo, Title, State, Grade string
	Number, Additions, Deletions   int
	Notes                          []string
}

// Preview is a server the Task runs that the person can open.
type Preview struct {
	Name, URL string
	Port      int
	Live      bool
}

// Link is a link an ExternalTask carries: a pull request, a ticket.
type Link struct{ Label, URL string }

// Template prefills the new-Task dialog.
type Template struct {
	Name, Host, Project, Cwd, Command, Model string
	Headless                                 bool
}

// Event is a journal event the plugin's events:<kind> scopes cover. Which
// of the Task's fields are set depends on Kind (pluginv1.JournalEvent says);
// From and To are Status words, From "" on task_created.
type Event struct {
	At   time.Time
	Host string
	// Workspace is the Host's Workspace; "" is its default one.
	Workspace                            string
	TaskID                               string
	Kind                                 string // status_changed, gate_opened, turn_finished, …
	TaskName, Project, From, To, Preview string
	// Journal is the event as the API carries it, every field included.
	Journal *pluginv1.JournalEvent
}

// The statuses, serai's words for where a Task is.
const (
	StatusUnderway  = "underway"
	StatusAtTheGate = "at_the_gate"
	StatusArrived   = "arrived"
	StatusStabled   = "stabled"
	StatusStranded  = "stranded"
	StatusNoAgent   = "no_agent"
)

var statusWords = map[pluginv1.TaskStatus]string{
	pluginv1.TaskStatus_TASK_STATUS_UNDERWAY:    StatusUnderway,
	pluginv1.TaskStatus_TASK_STATUS_AT_THE_GATE: StatusAtTheGate,
	pluginv1.TaskStatus_TASK_STATUS_ARRIVED:     StatusArrived,
	pluginv1.TaskStatus_TASK_STATUS_STABLED:     StatusStabled,
	pluginv1.TaskStatus_TASK_STATUS_STRANDED:    StatusStranded,
	pluginv1.TaskStatus_TASK_STATUS_NO_AGENT:    StatusNoAgent,
}

// StatusWord is a TaskStatus as the Status constants spell it; "" for
// unspecified.
func StatusWord(st pluginv1.TaskStatus) string { return statusWords[st] }

// Status reads a status event: the Task, its Project, and the statuses it
// moved between, each one of the Status constants.
func (e Event) Status() (task, project, from, to string) {
	return e.TaskName, e.Project, e.From, e.To
}

// Refused is the error a Handler returns to say no: the command exits as
// refused rather than failed.
func Refused(format string, args ...any) error {
	return pluginwire.Errorf(pluginv1.Code_CODE_REFUSED, format, args...)
}

// CodeOf is the API's code an error from a call carries: CODE_REFUSED for
// a scope or a rule, CODE_UNAVAILABLE for a Host that isn't connected, and
// so on; CODE_OK for nil.
func CodeOf(err error) pluginv1.Code { return pluginwire.CodeOf(err) }

// Main runs the plugin on stdin and stdout until the gateway closes the
// connection, and exits: 0 then, 1 when setup or the connection failed.
func Main(setup func(*Plugin) error) {
	if err := Run(context.Background(), setup); err != nil {
		log.Printf("plugin: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Run serves the plugin on stdin and stdout. stdout carries the protocol,
// so os.Stdout is pointed at stderr (the plugin's log) first: a stray
// fmt.Println must not corrupt a frame.
func Run(ctx context.Context, setup func(*Plugin) error) error {
	out := os.Stdout
	os.Stdout = os.Stderr
	log.SetOutput(os.Stderr)
	return Serve(ctx, stdio{os.Stdin, out}, os.Getenv("SERAI_PLUGIN"), setup)
}

type stdio struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (s stdio) Read(b []byte) (int, error)  { return s.r.Read(b) }
func (s stdio) Write(b []byte) (int, error) { return s.w.Write(b) }
func (s stdio) Close() error                { return errors.Join(s.r.Close(), s.w.Close()) }

// sdkModule is the module path the SDK is published under: its version, as
// the plugin's build records it, goes in the Hello.
const sdkModule = "github.com/serai-sh/serai-pluginsdk"

func sdkVersion() string {
	v := "(devel)"
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == sdkModule {
				v = d.Version
			}
		}
	}
	return "go-sdk " + v
}

// Serve runs the plugin over rwc: the connection Run makes from stdio, or
// one a gateway serves in its own process, for a plugin compiled into
// serai. name goes into the Hello.
func Serve(ctx context.Context, rwc io.ReadWriteCloser, name string, setup func(*Plugin) error) error {
	p := &Plugin{handlers: map[string]Handler{}, events: make(chan Event, eventBuffer), crashed: make(chan error, 1)}
	p.ctx, p.cancel = context.WithCancel(ctx)
	defer func() {
		p.cancel()
		p.wg.Wait()
	}()
	p.c = pluginwire.New(rwc, pluginwire.Options{
		Hello:     &pluginv1.Hello{Name: name, Version: os.Getenv("SERAI_PLUGIN_VERSION"), Sdk: sdkVersion()},
		OnRequest: p.handle,
		OnEvent:   p.queueEvent,
	})
	defer p.c.Close()
	hctx, cancel := context.WithTimeout(ctx, helloTimeout)
	err := p.c.Ready(hctx)
	cancel()
	if err != nil {
		return fmt.Errorf("no Hello from the gateway: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	resp, err := p.c.Call(cctx, &pluginv1.Request{Call: &pluginv1.Request_GetConfig{GetConfig: &pluginv1.GetConfig{}}})
	cancel()
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}
	p.config = resp.GetConfig().GetValues()

	p.Go(p.dispatch)

	if err := setup(p); err != nil {
		return err
	}
	select {
	case <-p.c.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.crashed:
		return err
	}
}

// Config is the plugin's config: the user's values over the manifest's
// defaults.
func (p *Plugin) Config(key string) string { return p.config[key] }

// SeraiVersion is the version of the serai that runs the plugin, as its
// gateway said in its Hello: for a plugin naming itself to another system.
func (p *Plugin) SeraiVersion() string { return p.c.Peer().GetSeraiVersion() }

// Scopes are the scopes the person granted the plugin, as plugin.toml
// spells them.
func (p *Plugin) Scopes() []string { return p.c.Peer().GetScopes() }

// Command registers a command and the handler that runs it. It needs the
// commands scope.
func (p *Plugin) Command(cmd Command, h Handler) error {
	spec := &pluginv1.Command{Noun: cmd.Noun, Verb: cmd.Verb, Usage: cmd.Usage, Short: cmd.Short, Effect: effect(cmd.Effect)}
	for _, f := range cmd.Flags {
		spec.Flags = append(spec.Flags, &pluginv1.Flag{Name: f.Name, Type: f.Type, Usage: f.Usage, Default: f.Default, Required: f.Required})
	}
	p.mu.Lock()
	p.handlers[cmd.Noun+" "+cmd.Verb] = h
	p.mu.Unlock()
	return p.call(&pluginv1.Request{Call: &pluginv1.Request_RegisterCommand{RegisterCommand: &pluginv1.RegisterCommand{Command: spec}}})
}

func effect(e Effect) pluginv1.CommandEffect {
	switch e {
	case Read:
		return pluginv1.CommandEffect_COMMAND_EFFECT_READ
	case Write:
		return pluginv1.CommandEffect_COMMAND_EFFECT_WRITE
	case Local:
		return pluginv1.CommandEffect_COMMAND_EFFECT_LOCAL
	}
	return pluginv1.CommandEffect_COMMAND_EFFECT_CONFIRM
}

// PublishCard shows or replaces a Card. It needs the cards scope.
func (p *Plugin) PublishCard(ctx context.Context, c Card) error {
	card := &pluginv1.Card{Id: c.ID, Host: c.Host, TaskId: c.TaskID, Title: c.Title, Markdown: c.Markdown, Covers: c.Covers, Details: c.Details}
	for _, a := range c.Actions {
		card.Actions = append(card.Actions, &pluginv1.CardAction{Label: a.Label, Command: a.Command})
	}
	for _, b := range c.Badges {
		card.Badges = append(card.Badges, &pluginv1.Badge{Label: b.Label, Tone: pluginv1.Tone(b.Tone)})
	}
	return p.callCtx(ctx, &pluginv1.Request{Call: &pluginv1.Request_PublishCard{PublishCard: &pluginv1.PublishCard{Card: card}}})
}

// DeleteCard removes a Card.
func (p *Plugin) DeleteCard(ctx context.Context, id string) error {
	return p.callCtx(ctx, &pluginv1.Request{Call: &pluginv1.Request_DeleteCard{DeleteCard: &pluginv1.DeleteCard{Id: id}}})
}

var sourceStatus = map[string]pluginv1.TaskStatus{
	StatusStabled:   pluginv1.TaskStatus_TASK_STATUS_STABLED,
	StatusUnderway:  pluginv1.TaskStatus_TASK_STATUS_UNDERWAY,
	StatusAtTheGate: pluginv1.TaskStatus_TASK_STATUS_AT_THE_GATE,
	StatusArrived:   pluginv1.TaskStatus_TASK_STATUS_ARRIVED,
}

// PublishTasks replaces the plugin's Tasks from outside serai with s; an
// empty s shows none. It needs the source scope. The gateway holds every
// string to one line and every URL to http or https, and refuses the whole
// set otherwise.
func (p *Plugin) PublishTasks(ctx context.Context, s Source) error {
	src := &pluginv1.Source{Label: s.Label, Note: s.Note, Icon: icon(s.Icon), IconDark: icon(s.IconDark), Symbol: s.Symbol}
	for _, t := range s.Tasks {
		st, ok := sourceStatus[t.Status]
		if !ok {
			return fmt.Errorf("task %s: status %q is not stabled, underway, at the gate or arrived", t.ID, t.Status)
		}
		et := &pluginv1.SourceTask{Id: t.ID, Name: t.Name, Project: t.Project, Status: st, Url: t.URL, Preview: t.Preview,
			ConversationRev: t.ConversationRev, AcceptsMessages: t.AcceptsMessages, MessagesWhileWorking: t.MessagesWhileWorking, Stoppable: t.Stoppable,
			Archivable: t.Archivable, Leavable: t.Leavable,
			GroupId: t.Group, Links: links(t.Links), Artifacts: links(t.Artifacts),
			Participants: t.Participants, Model: t.Model, Cost: t.Cost, StateNote: t.StateNote,
			ContextPercent: int32(t.ContextPercent), ContextTokens: t.ContextTokens}
		if g := t.Gate; g != nil {
			et.Gate = &pluginv1.Gate{Id: g.ID, Tool: g.Tool, Input: g.Input, MayApprove: g.MayApprove, MayDeny: g.MayDeny, WhyNot: g.WhyNot, Body: gateBody(g.Body)}
		}
		if !t.LastActivity.IsZero() {
			et.LastActivityS = t.LastActivity.Unix()
		}
		for _, pl := range t.Plans {
			et.Plans = append(et.Plans, &pluginv1.Plan{Title: pl.Title, Url: pl.URL, Items: planItems(pl.Items)})
		}
		for _, pr := range t.PullRequests {
			et.PullRequests = append(et.PullRequests, &pluginv1.PullRequest{Url: pr.URL, Repo: pr.Repo, Number: int32(pr.Number), Title: pr.Title,
				State: pr.State, Additions: int32(pr.Additions), Deletions: int32(pr.Deletions), Grade: pr.Grade, Notes: pr.Notes})
		}
		for _, pv := range t.Previews {
			et.Previews = append(et.Previews, &pluginv1.Preview{Name: pv.Name, Url: pv.URL, Port: int32(pv.Port), Live: pv.Live})
		}
		src.Tasks = append(src.Tasks, et)
	}
	for _, g := range s.Groups {
		eg := &pluginv1.SourceGroup{Id: g.ID, Title: g.Title, LeadTaskId: g.Lead, Url: g.URL, Items: planItems(g.Items), ItemsWord: g.ItemsWord}
		if sum := g.Summary; sum != nil {
			eg.Summary = &pluginv1.GroupSummary{Subtitle: sum.Subtitle, Done: int32(sum.Done), Total: int32(sum.Total), DoneWord: sum.DoneWord, Bar: sum.Bar, State: sum.State, Link: link(sum.Link),
				Attention: int32(sum.Attention), AttentionWord: sum.AttentionWord}
		}
		src.Groups = append(src.Groups, eg)
	}
	return p.callCtx(ctx, &pluginv1.Request{Call: &pluginv1.Request_PublishTasks{PublishTasks: &pluginv1.PublishTasks{Source: src}}})
}

func planItems(its []PlanItem) []*pluginv1.PlanItem {
	var out []*pluginv1.PlanItem
	for _, it := range its {
		ei := &pluginv1.PlanItem{Label: it.Label, State: it.State, Done: it.Done, Url: it.URL, Kind: it.Kind, Note: it.Note, Attention: it.Attention}
		if !it.Since.IsZero() {
			ei.SinceS = it.Since.Unix()
		}
		out = append(out, ei)
	}
	return out
}

func gateBody(b *GateBody) *pluginv1.GateBody {
	if b == nil {
		return nil
	}
	out := &pluginv1.GateBody{Action: b.Action, Title: b.Title, Count: b.Count}
	for _, s := range b.Sections {
		out.Sections = append(out.Sections, &pluginv1.GateSection{Heading: s.Heading, Markdown: s.Markdown, Folded: s.Folded})
	}
	for _, it := range b.Items {
		out.Items = append(out.Items, &pluginv1.GateItem{Label: it.Label, Kind: it.Kind, Detail: it.Detail})
	}
	return out
}

func link(l *Link) *pluginv1.Link {
	if l == nil {
		return nil
	}
	return &pluginv1.Link{Label: l.Label, Url: l.URL}
}

func links(ls []Link) []*pluginv1.Link {
	var out []*pluginv1.Link
	for i := range ls {
		out = append(out, link(&ls[i]))
	}
	return out
}

// Template registers a Task template. It needs the commands scope.
func (p *Plugin) Template(t Template) error {
	return p.call(&pluginv1.Request{Call: &pluginv1.Request_RegisterTemplate{RegisterTemplate: &pluginv1.RegisterTemplate{Template: &pluginv1.Template{
		Name: t.Name, Host: t.Host, Project: t.Project, Cwd: t.Cwd, Command: t.Command, Headless: t.Headless, Model: t.Model,
	}}}})
}

// OnEvent calls fn with every journal event the plugin's events:<kind>
// scopes cover, one at a time, in order. Events that arrive while no
// handler keeps up are dropped after a buffer of 256.
func (p *Plugin) OnEvent(fn func(ctx context.Context, ev Event)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onEvent = append(p.onEvent, fn)
}

// ListHosts lists every Host and named Workspace the gateway drives, each
// as a Host of its own. It needs the read scope.
func (p *Plugin) ListHosts(ctx context.Context) ([]*pluginv1.Host, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListHosts{ListHosts: &pluginv1.ListHosts{}}})
	return resp.GetHostList().GetHosts(), err
}

// ListTasks lists one Host's (and Workspace's) Tasks. It needs read.
func (p *Plugin) ListTasks(ctx context.Context, req *pluginv1.ListTasks) ([]*pluginv1.Task, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListTasks{ListTasks: req}})
	return resp.GetTaskList().GetTasks(), err
}

// ReadConversation reads one page of a Task's conversation. It needs read.
func (p *Plugin) ReadConversation(ctx context.Context, req *pluginv1.ReadConversation) (*pluginv1.ConversationPage, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ReadConversation{ReadConversation: req}})
	return resp.GetConversationPage(), err
}

// GitStatus reads the git checkout a Task works in. It needs read.
func (p *Plugin) GitStatus(ctx context.Context, req *pluginv1.GitStatus) (*pluginv1.GitStatusResult, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_GitStatus{GitStatus: req}})
	return resp.GetGitStatus(), err
}

// ListPullRequests lists the pull requests serai's own work tab cards for
// a Task, in its order, each with the cover a Card names it by. It needs
// read. A serai older than the call answers CODE_UNIMPLEMENTED.
func (p *Plugin) ListPullRequests(ctx context.Context, req *pluginv1.ListPullRequests) (*pluginv1.PullRequestList, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListPullRequests{ListPullRequests: req}})
	return resp.GetPullRequestList(), err
}

// ListEvents reads the journal after a cursor. It needs read.
func (p *Plugin) ListEvents(ctx context.Context, req *pluginv1.ListEvents) (*pluginv1.EventList, error) {
	resp, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_ListEvents{ListEvents: req}})
	return resp.GetEventList(), err
}

// SendToTask hands a Task's agent text from the plugin, as a program's
// message: only a live agent not at a prompt takes it. It needs send.
func (p *Plugin) SendToTask(ctx context.Context, req *pluginv1.SendToTask) error {
	_, err := p.Call(ctx, &pluginv1.Request{Call: &pluginv1.Request_SendToTask{SendToTask: req}})
	return err
}

// Call sends any plugin API request, for a call the SDK doesn't wrap. A
// non-OK answer comes back as an error CodeOf reads, with the Response.
func (p *Plugin) Call(ctx context.Context, req *pluginv1.Request) (*pluginv1.Response, error) {
	return p.c.Call(ctx, req)
}

func (p *Plugin) call(req *pluginv1.Request) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	return p.callCtx(ctx, req)
}

func (p *Plugin) callCtx(ctx context.Context, req *pluginv1.Request) error {
	_, err := p.c.Call(ctx, req)
	return err
}

// SourceHandler serves what the person does with the plugin's Tasks from
// outside serai (PublishTasks), each nil when the source has none of it:
//
//   - Conversation is one page of a Task's conversation, oldest first;
//     cursor "" is the newest page, and the page's NextCursor the one
//     before it. Offered for a Task published with a ConversationRev.
//   - Send hands the source a message the person typed for a Task, as
//     typed. Offered for a Task published with AcceptsMessages. Return
//     Refused(...) in words the person reads as they are.
//   - Seen says the person looked at a Task: clear its arrived.
//   - Answer is the person's approve (true) or deny of the Gate a Task was
//     published with, by its ID. The gateway has checked the Task is at
//     the gate with that Gate and that the Gate lets the person answer so.
//   - Stop is the person's stop of a Stoppable Task's running turn.
//   - Archive is the person's archive of an Archivable Task in the source,
//     for everyone in it; Leave takes a Leavable Task off the person's own
//     lists there. Drop the Task from the next PublishTasks once it is
//     done, and return Refused(...) in the source's words when it won't.
//
// The gateway calls them only for a Task the plugin shows, and only for the
// person's own frontends, never for MCP, another plugin or a relay.
type SourceHandler struct {
	Conversation func(ctx context.Context, taskID, cursor string) (*pluginv1.ConversationPage, error)
	Send         func(ctx context.Context, taskID, text string) error
	Seen         func(ctx context.Context, taskID string) error
	Answer       func(ctx context.Context, taskID, gateID string, approve bool) error
	Stop         func(ctx context.Context, taskID string) error
	Archive      func(ctx context.Context, taskID string) error
	Leave        func(ctx context.Context, taskID string) error
}

// ServeSource answers the gateway's calls about the plugin's Tasks with h.
func (p *Plugin) ServeSource(h SourceHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.source = h
}

func fail(err error) *pluginv1.Response {
	var pe *pluginwire.Error
	if !errors.As(err, &pe) {
		pe = pluginwire.Errorf(pluginv1.Code_CODE_INTERNAL, "%v", err)
	}
	return pluginwire.Fail(pe)
}

func (p *Plugin) handleSource(ctx context.Context, req *pluginv1.Request) *pluginv1.Response {
	p.mu.Lock()
	h := p.source
	p.mu.Unlock()
	unserved := func(what string) *pluginv1.Response {
		return pluginwire.Fail(pluginwire.Errorf(pluginv1.Code_CODE_UNIMPLEMENTED, "this source serves no %s", what))
	}
	done := func(err error) *pluginv1.Response {
		if err != nil {
			return fail(err)
		}
		return pluginwire.OK()
	}
	switch c := req.Call.(type) {
	case *pluginv1.Request_ReadSourceConversation:
		if h.Conversation == nil {
			return unserved("conversation")
		}
		page, err := h.Conversation(ctx, c.ReadSourceConversation.GetTaskId(), c.ReadSourceConversation.GetCursor())
		if err != nil {
			return fail(err)
		}
		return &pluginv1.Response{Result: &pluginv1.Response_ConversationPage{ConversationPage: page}}
	case *pluginv1.Request_SendToSource:
		if h.Send == nil {
			return unserved("messages")
		}
		return done(h.Send(ctx, c.SendToSource.GetTaskId(), c.SendToSource.GetText()))
	case *pluginv1.Request_SourceTaskSeen:
		if h.Seen == nil {
			return pluginwire.OK()
		}
		return done(h.Seen(ctx, c.SourceTaskSeen.GetTaskId()))
	case *pluginv1.Request_AnswerSourceGate:
		if h.Answer == nil {
			return unserved("answers")
		}
		a := c.AnswerSourceGate
		return done(h.Answer(ctx, a.GetTaskId(), a.GetGateId(), a.GetApprove()))
	case *pluginv1.Request_StopSourceTask:
		if h.Stop == nil {
			return unserved("stop")
		}
		return done(h.Stop(ctx, c.StopSourceTask.GetTaskId()))
	case *pluginv1.Request_ArchiveSourceTask:
		if h.Archive == nil {
			return unserved("archive")
		}
		return done(h.Archive(ctx, c.ArchiveSourceTask.GetTaskId()))
	case *pluginv1.Request_LeaveSourceTask:
		if h.Leave == nil {
			return unserved("leave")
		}
		return done(h.Leave(ctx, c.LeaveSourceTask.GetTaskId()))
	}
	return nil
}

// handle answers the gateway: InvokeCommand, and the calls about the
// plugin's Tasks from outside serai (ServeSource).
func (p *Plugin) handle(ctx context.Context, req *pluginv1.Request) *pluginv1.Response {
	if resp := p.handleSource(ctx, req); resp != nil {
		return resp
	}
	inv := req.GetInvokeCommand()
	if inv == nil {
		return nil
	}
	p.mu.Lock()
	h := p.handlers[inv.GetNoun()+" "+inv.GetVerb()]
	p.mu.Unlock()
	if h == nil {
		return pluginwire.Fail(pluginwire.Errorf(pluginv1.Code_CODE_NOT_FOUND, "no command %s %s", inv.GetNoun(), inv.GetVerb()))
	}
	out, err := h(ctx, Invocation{Args: inv.GetArgs(), Flags: inv.GetFlags(), Caller: inv.GetCaller()})
	if err != nil {
		return fail(err)
	}
	return &pluginv1.Response{Result: &pluginv1.Response_CommandOutput{CommandOutput: &pluginv1.CommandOutput{Text: out}}}
}

func (p *Plugin) queueEvent(ev *pluginv1.Event) {
	je := ev.GetJournalEvent()
	if je == nil {
		return
	}
	e := Event{At: time.UnixMilli(je.GetAtMs()), Host: je.GetHost(), Workspace: je.GetWorkspace(), TaskID: je.GetTaskId(), Kind: je.GetKind(),
		TaskName: je.GetTaskName(), Project: je.GetProject(), From: StatusWord(je.GetFrom()), To: StatusWord(je.GetTo()), Preview: je.GetPreview(), Journal: je}
	select {
	case p.events <- e:
	default:
		log.Printf("plugin: event handlers are behind; dropped a %s event", e.Kind)
	}
}

func (p *Plugin) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-p.events:
			p.mu.Lock()
			fns := append([]func(context.Context, Event){}, p.onEvent...)
			p.mu.Unlock()
			for _, fn := range fns {
				p.runEvent(ctx, fn, ev)
			}
		}
	}
}
