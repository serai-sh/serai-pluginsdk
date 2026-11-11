// SPDX-License-Identifier: Apache-2.0

// Command hello is serai's example plugin (serai's docs/PLUGINS.md) as a program of
// its own. Its code is in package hello, which an edition can compile in
// instead (editions/example).
package main

import (
	"github.com/serai-sh/serai-pluginsdk"
	"github.com/serai-sh/serai-pluginsdk/examples/plugins/hello/hello"
)

func main() {
	pluginsdk.Main(hello.Setup)
}
