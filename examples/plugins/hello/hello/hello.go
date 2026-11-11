// SPDX-License-Identifier: Apache-2.0

// Package hello is the example plugin's code (serai's docs/PLUGINS.md), apart from
// its main so that an edition can compile it in: `serai hello greet`, a
// Card on every Task whose status changed, and a Task template, using only
// pkg/pluginsdk. editions/example/edition.toml builds it in.
package hello

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/serai-sh/serai-pluginsdk"
)

// Setup registers everything the plugin does. It is what an edition's
// [[plugins]] go = "…/hello/hello" entry calls.
func Setup(p *pluginsdk.Plugin) error {
	greet := pluginsdk.Command{
		Noun: "hello", Verb: "greet", Usage: "[who]...", Short: "Say hello",
		Flags:  []pluginsdk.Flag{{Name: "name", Usage: "who to greet", Default: "world"}},
		Effect: pluginsdk.Read,
	}
	err := p.Command(greet, func(_ context.Context, inv pluginsdk.Invocation) (string, error) {
		who := inv.Flags["name"]
		if len(inv.Args) > 0 {
			who = strings.Join(inv.Args, " and ")
		}
		return fmt.Sprintf("%s, %s!", p.Config("greeting"), who), nil
	})
	if err != nil {
		return err
	}
	if err := p.Template(pluginsdk.Template{Name: "hello", Project: "hello", Command: "claude"}); err != nil {
		return err
	}
	p.OnEvent(func(ctx context.Context, ev pluginsdk.Event) {
		if ev.TaskID == "" {
			return
		}
		task, project, from, to := ev.Status()
		err := p.PublishCard(ctx, pluginsdk.Card{
			ID: "status-" + ev.TaskID, Host: ev.Host, TaskID: ev.TaskID, Title: "hello",
			Markdown: fmt.Sprintf("**%s** in %s went from _%s_ to **%s**.", task, project, from, to),
			Actions:  []pluginsdk.Action{{Label: "Greet " + task, Command: "hello greet --name " + strconv.Quote(task)}},
		})
		if err != nil {
			log.Printf("publishing a Card: %v", err)
		}
	})
	return nil
}
