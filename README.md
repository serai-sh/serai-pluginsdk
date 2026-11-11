# serai plugin SDK

serai drives Claude Code and OpenCode agents that run in tmux on your
machines. A **plugin** extends it: a program serai's gateway runs on your computer, which adds
commands (`serai <noun> <verb>`, and MCP tools for the agents when the command allows it), Cards
(Markdown with buttons beside a Task), Task templates, and Tasks from outside serai. It can follow
what happens across your Tasks, such as status changes, gates and finished turns, and it can do
only what the scopes you accepted allow.

This module is the Go SDK for writing one, and the plugin API it speaks, `serai.plugin.v1`:

| Path | What |
|---|---|
| `github.com/serai-sh/serai-pluginsdk` | package `pluginsdk`: connect, register, read, send, serve |
| `gen/go/serai/plugin/v1` | the API's Go types (`pluginv1`) |
| `proto/serai/plugin/v1` | the API itself, for a plugin in another language |
| `examples/plugins/hello` | a complete plugin: a command, a Card on every status change, a template |

The API is all a plugin sees of serai: serai translates it to its own internals, which change
without a plugin noticing. It is published from serai's own source when the API or the SDK
changes, so a pull request here can't be merged as it is; open an issue instead.

## A plugin in two files

```sh
mkdir greet && cd greet
go mod init example.com/greet
go get github.com/serai-sh/serai-pluginsdk@latest
```

`main.go`:

```go
package main

import (
	"context"

	"github.com/serai-sh/serai-pluginsdk"
)

func main() {
	pluginsdk.Main(func(p *pluginsdk.Plugin) error {
		greet := pluginsdk.Command{
			Noun: "greet", Verb: "say", Short: "Say hello",
			Flags:  []pluginsdk.Flag{{Name: "name", Default: "world"}},
			Effect: pluginsdk.Read,
		}
		return p.Command(greet, func(ctx context.Context, inv pluginsdk.Invocation) (string, error) {
			return p.Config("greeting") + ", " + inv.Flags["name"] + "!", nil
		})
	})
}
```

`plugin.toml`, the manifest serai reads before it runs anything:

```toml
name = "greet"                  # lowercase letters, digits, dashes; at most 40
version = "0.1.0"               # semantic version
description = "Says hello"
exec = "bin/greet"              # relative to this directory
scopes = ["commands"]

[config]
greeting = { type = "string", default = "Hello", description = "what greet say prints" }
```

```sh
go build -o bin/greet .
serai plugin add .
serai plugin enable greet       # shows the scopes in words and asks
serai greet say --name you      # Hello, you!
```

The gateway runs the program with this directory as its working directory, and restarts it with
a backoff when it exits. Its stderr goes to `~/.serai/v2/plugins/greet.log`. `Main` points
`os.Stdout` at stderr before it starts, because stdout carries the protocol: log with `log` or
`fmt.Fprintln(os.Stderr, …)`.

## What the SDK offers

- `Command(spec, handler)`: `serai <noun> <verb>`, and `:<noun> <verb>` in serai's TUI. The
  handler gets the positional arguments, every flag's value (defaults included, as strings) and
  the caller (`cli`, `tui`, `mcp`), and returns the text to print. Return `Refused(...)` to say
  no; any other error fails the command. `Effect` decides whether agents see it as an MCP tool:
  `Read` (runs freely, audited), `Write` (audited), `Confirm` (the zero value: asks the person
  every time) or `Local` (no tool at all).
- `PublishCard` / `DeleteCard`: Markdown on one Task (`Host` and `TaskID`), on a Host (`Host`), or
  everywhere. Publishing an ID again replaces the Card. An `Action` is a button that runs one of
  this plugin's own commands, never anyone else's.
- `Template`: a prefill for the new-Task dialog.
- `OnEvent`: the journal events the plugin's `events:<kind>` scopes cover, one at a time, in
  order. `Event.Status()` reads a status change: the Task, its Project, and the statuses it moved
  between (`StatusUnderway`, `StatusAtTheGate`, `StatusArrived`, `StatusStabled`,
  `StatusStranded`, `StatusNoAgent`).
- `PublishTasks` and `ServeSource`: Tasks from another system (a cloud agent's sessions, say)
  shown beside serai's own, in Groups if they belong together, with as much as the source serves:
  their conversation, the person's messages, an answer at their gate, a stop.
- `ListHosts`, `ListTasks`, `ReadConversation`, `GitStatus` and `ListEvents` read the fleet:
  every Host and Workspace, a Host's Tasks (name, Project, folder, status, agent), a Task's
  conversation in the same entries whichever agent wrote it, the git checkout a Task works in,
  and the journal. `SendToTask` hands a Task's agent text. Each takes the API's own request
  message, with the Host (and Workspace) it is for; `Call` sends any other v1 request, and
  `CodeOf(err)` reads why one failed (`CODE_REFUSED` for a scope or a rule,
  `CODE_UNAVAILABLE` for a Host that isn't connected).
- `SeraiVersion()` and `Scopes()`: the serai serving the plugin, and what the person granted.
- `Config(key)`: the person's value over the manifest's default. A `secret` key is set by the
  person (`serai plugin config set greet token`, asked without echo) and reaches only this
  plugin, through this call: never its environment or its command line.
- `Go(fn)`: a goroutine the plugin owns. Its context ends when the plugin stops, and a panic in
  it restarts the plugin instead of crashing the process.

## Scopes

| Scope | Allows |
|---|---|
| `read` | ListHosts, ListTasks, ReadConversation, GitStatus, ListEvents |
| `send` | SendToTask, to a live agent only: never a shell, a stranded Task or one waiting at a prompt. Audited. |
| `cards` | PublishCard, DeleteCard |
| `commands` | RegisterCommand, RegisterTemplate |
| `source` | PublishTasks, and answering the person's calls about those Tasks |
| `events:<kind>` | journal events of that kind: `status_changed`, `gate_opened`, `question_asked`, `turn_finished`, `task_created`, `task_archived`, `host_state`, `task_seen`, `tasks_restored`, `todo_changed`; `events:status` is `status_changed`, `gate_opened` and `turn_finished` |

serai also knows `tasks` (start and organise Tasks) and `manage` (kill, archive) for plugins built
before this API; v1 has no calls for them yet, and gets them as a plugin needs them.

Every plugin may read its own config. Ask for the fewest scopes you need: the person reads them
before they enable the plugin, and a new version that asks for one more stops running until they
accept it.

Whatever it was granted, a plugin never answers a permission prompt or a question, sends keys,
types into a shell, starts a Task with anything but an agent, deletes or migrates Tasks, manages
Hosts or plugins, or invokes another plugin's commands. Scopes limit what a plugin does *through
serai*; they are no sandbox. A plugin is a program running as you, so install the ones you would
install as any other program.

## In another language

Speak the API on stdin and stdout yourself. Each frame is a 4-byte big-endian length and a
protobuf `serai.plugin.v1.Frame` (`proto/serai/plugin/v1/plugin.proto`), at most 1 MiB.

1. Send `Frame{hello: {api: "serai.plugin.v1", name, version, sdk}}`. The gateway answers with
   its own Hello (your name and version, `serai_version`, the granted `scopes`), and drops a
   plugin that hasn't said one within 10 s.
2. Call `GetConfig` for your config.
3. Register: `RegisterCommand`, `RegisterTemplate`, `PublishCard`, `PublishTasks`.
4. Answer the gateway's `InvokeCommand` with `Response{command_output: {text}}`, or a `Status`
   (`CODE_REFUSED` to say no), and its calls about your Task source.
5. Receive `Event{journal_event}` for your `events:` scopes.
6. Read and send with `ListHosts`, `ListTasks`, `ReadConversation`, `GitStatus`, `ListEvents` and
   `SendToTask`, each naming the Host it is for.

The `.proto` file says what every message and field means.

## Versions

- **The API, `serai.plugin.v1`, only grows**: new messages, fields, calls and enum values;
  nothing is renamed, renumbered, retyped or removed, and serai checks that on every change. A
  break would be `serai.plugin.v2`, which serai would serve beside v1 for an overlap of at least
  a release. Ignore fields you don't know; treat an enum value you don't know as its zero value.
  A plugin built with an older SDK keeps working with a newer serai; one built with a newer SDK
  works with an older serai for what it already knew, and a call it doesn't is answered
  `CODE_UNIMPLEMENTED` and nothing runs.
- **The SDK has its own semantic version**, not serai's build number: it changes only when the
  API or the SDK does. It is v0.x until serai declares the API stable, then v1.x.y, its major
  following the API's (`serai.plugin.v2` would be the module's `/v2`).

## License

Apache License 2.0; see LICENSE and NOTICE.
