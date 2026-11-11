// SPDX-License-Identifier: Apache-2.0

// Package pluginwire carries the plugin API (serai.plugin.v1,
// proto/serai/plugin/v1) over a byte stream: a plugin's stdin and stdout, or
// a pipe in the gateway's own process. Both ends use it, the SDK on the
// plugin's side and internal/pluginapi on the gateway's.
//
// A frame is a 4-byte big-endian length followed by an encoded
// pluginv1.Frame, at most MaxFrame. Conn adds the rest: the Hello exchange,
// requests matched to their responses by id, cancellation, and a writer of
// its own so that reading never waits on writing.
package pluginwire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pluginv1 "github.com/serai-sh/serai-pluginsdk/gen/go/serai/plugin/v1"
)

// API is the version this package speaks, as a Hello names it.
const API = "serai.plugin.v1"

// MaxFrame bounds one frame; a peer announcing more is broken, not trusted
// with the allocation.
const MaxFrame = 1 << 20

// DefaultCallTimeout bounds a Call whose context has no deadline.
const DefaultCallTimeout = 30 * time.Second

// maxHandling bounds the peer's requests being handled at once; one more is
// answered UNAVAILABLE rather than queued without end.
const maxHandling = 64

// queueLimit bounds the frames waiting for the writer; a sender blocks while
// it is full, which is the backpressure a slow peer deserves.
const queueLimit = 1024

var (
	ErrClosed        = errors.New("pluginwire: connection closed")
	ErrFrameTooLarge = errors.New("pluginwire: frame exceeds MaxFrame")
)

// Error is a non-OK Status: the peer's answer, or one made here for a call
// that never got one.
type Error struct {
	Code    pluginv1.Code
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code.String()
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Errorf builds an Error.
func Errorf(code pluginv1.Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf is the code err carries: CODE_OK for nil, CODE_INTERNAL for an
// error that carries none.
func CodeOf(err error) pluginv1.Code {
	if err == nil {
		return pluginv1.Code_CODE_OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return pluginv1.Code_CODE_INTERNAL
}

// Fail is the Response for err, keeping its code when it has one.
func Fail(err error) *pluginv1.Response {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Code: pluginv1.Code_CODE_INTERNAL, Message: err.Error()}
	}
	return &pluginv1.Response{Status: &pluginv1.Status{Code: e.Code, Message: e.Message}}
}

// OK is an empty successful Response.
func OK() *pluginv1.Response {
	return &pluginv1.Response{Result: &pluginv1.Response_Empty{Empty: &pluginv1.Empty{}}}
}

// WriteFrame writes one frame with a single Write.
func WriteFrame(w io.Writer, f *pluginv1.Frame) error {
	body, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	if len(body) > MaxFrame {
		return ErrFrameTooLarge
	}
	buf := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body)))
	_, err = w.Write(append(buf, body...))
	return err
}

// ReadRaw reads one frame's bytes, without decoding them.
func ReadRaw(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrame {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return body, nil
}

// ReadFrame reads and decodes one frame.
func ReadFrame(r io.Reader) (*pluginv1.Frame, error) {
	body, err := ReadRaw(r)
	if err != nil {
		return nil, err
	}
	f := &pluginv1.Frame{}
	if err := proto.Unmarshal(body, f); err != nil {
		return nil, fmt.Errorf("pluginwire: decode frame: %w", err)
	}
	return f, nil
}

// Handler answers one of the peer's requests. ctx ends when the peer
// cancels it or the connection closes. A nil Response is UNIMPLEMENTED.
type Handler func(ctx context.Context, req *pluginv1.Request) *pluginv1.Response

type Options struct {
	// Hello is sent first. Its api is set to API.
	Hello *pluginv1.Hello
	// OnRequest answers the peer's requests; nil answers UNIMPLEMENTED.
	OnRequest Handler
	// OnEvent gets the peer's Events, one at a time on the reader: it must
	// not block. Nil drops them.
	OnEvent func(*pluginv1.Event)
}

// Conn is one connection. Its methods are safe for concurrent use.
type Conn struct {
	rwc io.ReadWriteCloser
	opt Options

	ctx    context.Context // ends on close; parent of every handler's
	cancel context.CancelFunc

	out    chan *pluginv1.Frame
	closed chan struct{}
	ready  chan struct{}

	mu       sync.Mutex
	err      error
	peer     *pluginv1.Hello
	nextID   uint64
	pending  map[uint64]chan *pluginv1.Response
	handling map[uint64]context.CancelFunc
}

// New starts a connection over rwc: it sends opt.Hello and reads until the
// connection ends. The Conn owns rwc and closes it.
func New(rwc io.ReadWriteCloser, opt Options) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		rwc: rwc, opt: opt, ctx: ctx, cancel: cancel,
		out:      make(chan *pluginv1.Frame, queueLimit),
		closed:   make(chan struct{}),
		ready:    make(chan struct{}),
		pending:  map[uint64]chan *pluginv1.Response{},
		handling: map[uint64]context.CancelFunc{},
	}
	hello := &pluginv1.Hello{}
	if opt.Hello != nil {
		hello = proto.Clone(opt.Hello).(*pluginv1.Hello)
	}
	hello.Api = API
	c.out <- &pluginv1.Frame{Body: &pluginv1.Frame_Hello{Hello: hello}}
	go c.writeLoop()
	go c.readLoop()
	return c
}

// Ready waits for the peer's Hello.
func (c *Conn) Ready(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-c.closed:
		return c.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Peer is the peer's Hello; nil before it arrived.
func (c *Conn) Peer() *pluginv1.Hello {
	select {
	case <-c.ready:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.peer
	default:
		return nil
	}
}

// Done is closed when the connection ends.
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Err is why it ended: nil while open, ErrClosed after Close.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close ends the connection.
func (c *Conn) Close() error {
	c.closeWith(ErrClosed)
	return nil
}

func (c *Conn) closeWith(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	c.mu.Unlock()
	c.cancel()
	close(c.closed)
	_ = c.rwc.Close()
}

// Call sends req and waits for its answer. A non-OK status comes back as an
// *Error, with the Response.
func (c *Conn) Call(ctx context.Context, req *pluginv1.Request) (*pluginv1.Response, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultCallTimeout)
		defer cancel()
	}
	ch := make(chan *pluginv1.Response, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return nil, Errorf(pluginv1.Code_CODE_UNAVAILABLE, "%v", c.err)
	}
	c.nextID++
	id := c.nextID
	c.pending[id] = ch
	c.mu.Unlock()
	if err := c.send(ctx, &pluginv1.Frame{Id: id, Body: &pluginv1.Frame_Request{Request: req}}); err != nil {
		c.forget(id)
		return nil, err
	}
	select {
	case <-c.closed:
		c.forget(id)
		return nil, Errorf(pluginv1.Code_CODE_UNAVAILABLE, "connection closed: %v", c.Err())
	case resp := <-ch:
		if st := resp.GetStatus(); st.GetCode() != pluginv1.Code_CODE_OK {
			return resp, &Error{Code: st.GetCode(), Message: st.GetMessage()}
		}
		return resp, nil
	case <-ctx.Done():
		c.forget(id)
		select {
		case c.out <- &pluginv1.Frame{Id: id, Body: &pluginv1.Frame_Cancel{Cancel: &pluginv1.Cancel{Id: id}}}:
		default:
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, Errorf(pluginv1.Code_CODE_DEADLINE, "no answer before the deadline")
		}
		return nil, ctx.Err()
	}
}

// Notify sends an Event.
func (c *Conn) Notify(ctx context.Context, ev *pluginv1.Event) error {
	return c.send(ctx, &pluginv1.Frame{Body: &pluginv1.Frame_Event{Event: ev}})
}

func (c *Conn) forget(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Conn) send(ctx context.Context, f *pluginv1.Frame) error {
	select {
	case c.out <- f:
		return nil
	case <-c.closed:
		return Errorf(pluginv1.Code_CODE_UNAVAILABLE, "connection closed: %v", c.Err())
	case <-ctx.Done():
		return Errorf(pluginv1.Code_CODE_DEADLINE, "the connection is not keeping up: %v", ctx.Err())
	}
}

func (c *Conn) writeLoop() {
	for {
		select {
		case f := <-c.out:
			if err := WriteFrame(c.rwc, f); err != nil {
				c.closeWith(fmt.Errorf("pluginwire: write: %w", err))
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) readLoop() {
	first := true
	for {
		f, err := ReadFrame(c.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = ErrClosed
			}
			c.closeWith(err)
			return
		}
		if first {
			h := f.GetHello()
			if h == nil {
				c.closeWith(errors.New("pluginwire: the peer's first frame is not a Hello"))
				return
			}
			if h.GetApi() != API {
				c.closeWith(fmt.Errorf("pluginwire: the peer speaks %q; this end speaks %s", h.GetApi(), API))
				return
			}
			c.mu.Lock()
			c.peer = h
			c.mu.Unlock()
			close(c.ready)
			first = false
			continue
		}
		c.dispatch(f)
	}
}

func (c *Conn) dispatch(f *pluginv1.Frame) {
	switch b := f.Body.(type) {
	case *pluginv1.Frame_Response:
		c.mu.Lock()
		ch := c.pending[f.Id]
		delete(c.pending, f.Id)
		c.mu.Unlock()
		if ch != nil {
			ch <- b.Response
		}
	case *pluginv1.Frame_Request:
		c.handle(f.Id, b.Request)
	case *pluginv1.Frame_Cancel:
		c.mu.Lock()
		cancel := c.handling[b.Cancel.GetId()]
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	case *pluginv1.Frame_Event:
		if c.opt.OnEvent != nil {
			c.opt.OnEvent(b.Event)
		}
	}
}

func (c *Conn) handle(id uint64, req *pluginv1.Request) {
	ctx, cancel := context.WithCancel(c.ctx)
	c.mu.Lock()
	busy := len(c.handling) >= maxHandling
	if !busy {
		c.handling[id] = cancel
	}
	c.mu.Unlock()
	if busy {
		cancel()
		c.reply(id, Fail(Errorf(pluginv1.Code_CODE_UNAVAILABLE, "%d requests are being handled already", maxHandling)))
		return
	}
	go func() {
		defer func() {
			c.mu.Lock()
			delete(c.handling, id)
			c.mu.Unlock()
			cancel()
		}()
		resp := c.call(ctx, req)
		if ctx.Err() != nil && c.ctx.Err() == nil {
			return // cancelled: the peer has stopped waiting for it
		}
		c.reply(id, resp)
	}()
}

// call runs the handler, recovering a panic in it: a plugin served in the
// gateway's own process must not take the gateway down with one handler.
func (c *Conn) call(ctx context.Context, req *pluginv1.Request) (resp *pluginv1.Response) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pluginwire: panic answering a request: %v\n%s", r, debug.Stack())
			resp = Fail(Errorf(pluginv1.Code_CODE_INTERNAL, "panicked: %v", r))
		}
	}()
	if c.opt.OnRequest != nil {
		resp = c.opt.OnRequest(ctx, req)
	}
	if resp == nil {
		resp = Fail(Errorf(pluginv1.Code_CODE_UNIMPLEMENTED, "this end answers no such request"))
	}
	return resp
}

func (c *Conn) reply(id uint64, resp *pluginv1.Response) {
	f := &pluginv1.Frame{Id: id, Body: &pluginv1.Frame_Response{Response: resp}}
	if proto.Size(f) > MaxFrame-64 {
		f.Body = &pluginv1.Frame_Response{Response: Fail(Errorf(pluginv1.Code_CODE_INTERNAL, "the answer is over %d bytes", MaxFrame))}
	}
	_ = c.send(c.ctx, f)
}
