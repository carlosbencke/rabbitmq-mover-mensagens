package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnSpec holds the parsed connection and queue specification.
type ConnSpec struct {
	User, Password string
	Host           string
	Port           int
	VHost          string
	Queue          string
}

// parseConnSpec parses a connection string of the form:
//
//	[user:pass@]host[:port][/vhost]/queue
//
// Examples:
//
//	localhost/fila1
//	guest:pass@localhost/fila1
//	guest:pass@rabbit.io/meu-vhost/fila1
//	guest:pass@rabbit.io:5673/meu-vhost/fila1
func parseConnSpec(raw string) (*ConnSpec, error) {
	spec := &ConnSpec{
		User:     "guest",
		Password: "guest",
		Port:     5672,
		VHost:    "/",
	}
	s := raw

	// Extract optional credentials (user:pass@)
	if at := strings.LastIndex(s, "@"); at >= 0 {
		creds := s[:at]
		s = s[at+1:]
		if colon := strings.Index(creds, ":"); colon >= 0 {
			spec.User = creds[:colon]
			spec.Password = creds[colon+1:]
		} else {
			spec.User = creds
		}
	}

	// Split host[:port] from /path
	slash := strings.Index(s, "/")
	if slash < 0 {
		return nil, fmt.Errorf("invalid spec %q: expected format [user:pass@]host[:port][/vhost]/queue", raw)
	}
	hostPart := s[:slash]
	pathPart := s[slash+1:] // strip leading slash

	// Parse host and optional port
	if colon := strings.LastIndex(hostPart, ":"); colon >= 0 {
		p, err := strconv.Atoi(hostPart[colon+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid port in %q: %v", raw, err)
		}
		spec.Host = hostPart[:colon]
		spec.Port = p
	} else {
		spec.Host = hostPart
	}

	if spec.Host == "" {
		return nil, fmt.Errorf("empty host in %q", raw)
	}

	// Parse [vhost/]queue
	parts := strings.SplitN(pathPart, "/", 2)
	switch len(parts) {
	case 1:
		spec.Queue = parts[0]
	case 2:
		spec.VHost = parts[0]
		spec.Queue = parts[1]
	}

	if spec.Queue == "" {
		return nil, fmt.Errorf("empty queue name in %q", raw)
	}
	return spec, nil
}

func (c *ConnSpec) amqpURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		url.QueryEscape(c.User),
		url.QueryEscape(c.Password),
		c.Host,
		c.Port,
		url.QueryEscape(c.VHost),
	)
}

func (c *ConnSpec) label() string {
	vhost := c.VHost
	if vhost == "/" {
		vhost = ""
	} else {
		vhost = "/" + vhost
	}
	return fmt.Sprintf("%s:%d%s/%s", c.Host, c.Port, vhost, c.Queue)
}

// Config holds the runtime configuration.
type Config struct {
	Src, Dst *ConnSpec
	Copy     bool
	Count    int64
	Workers  int
	Timeout  time.Duration
}

func toPublishing(d amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:         copyTable(d.Headers),
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}

func copyTable(t amqp.Table) amqp.Table {
	if t == nil {
		return nil
	}
	out := make(amqp.Table, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}

func run(cfg *Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcConn, err := amqp.Dial(cfg.Src.amqpURL())
	if err != nil {
		return fmt.Errorf("connect to source %s: %w", cfg.Src.Host, err)
	}
	defer srcConn.Close()

	dstConn, err := amqp.Dial(cfg.Dst.amqpURL())
	if err != nil {
		return fmt.Errorf("connect to destination %s: %w", cfg.Dst.Host, err)
	}
	defer dstConn.Close()

	// Inspect source queue to get available message count
	inspCh, err := srcConn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	q, err := inspCh.QueueInspect(cfg.Src.Queue)
	inspCh.Close()
	if err != nil {
		return fmt.Errorf("inspect source queue %q: %w", cfg.Src.Queue, err)
	}

	available := int64(q.Messages)
	if available == 0 {
		fmt.Println("Source queue is empty, nothing to do.")
		return nil
	}

	// Copy mode: republishing to source would cause an infinite loop if we don't
	// cap the count. Force --count to the snapshot taken above when not set.
	if cfg.Copy && cfg.Count == 0 {
		cfg.Count = available
	}

	// Total to display in progress bar
	total := available
	if cfg.Count > 0 && cfg.Count < total {
		total = cfg.Count
	}

	mode := "Moving"
	if cfg.Copy {
		mode = "Copying"
	}
	fmt.Printf("%s %d messages\n  from: %s\n    to: %s\n\n",
		mode, total, cfg.Src.label(), cfg.Dst.label())

	var (
		processed    int64
		remaining    int64 = cfg.Count // used only when Count > 0
		lastActivity int64 = time.Now().UnixNano()
	)

	// Watchdog: cancel context after cfg.Timeout of inactivity
	go func() {
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				since := time.Since(time.Unix(0, atomic.LoadInt64(&lastActivity)))
				if since >= cfg.Timeout {
					cancel()
					return
				}
			}
		}
	}()

	// Progress reporter (prints to same line via \r)
	progDone := make(chan struct{})
	go func() {
		defer close(progDone)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				printProgress(&processed, total, start)
				fmt.Println()
				return
			case <-tick.C:
				printProgress(&processed, total, start)
			}
		}
	}()

	prefetch := cfg.Workers * 4
	var wg sync.WaitGroup

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if werr := workerRun(
				ctx, cancel,
				srcConn, dstConn,
				cfg,
				&processed, &remaining, &lastActivity,
				prefetch,
			); werr != nil {
				fmt.Fprintf(os.Stderr, "\nworker error: %v\n", werr)
				cancel()
			}
		}()
	}

	wg.Wait()
	cancel()
	<-progDone

	n := atomic.LoadInt64(&processed)
	verb := "moved"
	if cfg.Copy {
		verb = "copied"
	}
	fmt.Printf("Done. %d messages %s.\n", n, verb)
	return nil
}

func workerRun(
	ctx context.Context,
	cancel context.CancelFunc,
	srcConn, dstConn *amqp.Connection,
	cfg *Config,
	processed, remaining *int64,
	lastActivity *int64,
	prefetch int,
) error {
	// Source channel for consuming
	srcCh, err := srcConn.Channel()
	if err != nil {
		return fmt.Errorf("open source channel: %w", err)
	}
	defer srcCh.Close()

	if err := srcCh.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("set QoS: %w", err)
	}

	// Destination channel with publisher confirms
	dstCh, err := dstConn.Channel()
	if err != nil {
		return fmt.Errorf("open dest channel: %w", err)
	}
	defer dstCh.Close()

	if err := dstCh.Confirm(false); err != nil {
		return fmt.Errorf("set confirm mode: %w", err)
	}
	confirms := dstCh.NotifyPublish(make(chan amqp.Confirmation, prefetch))

	// Extra source channel for copy mode (republish to origin)
	var srcCopyFn func(amqp.Publishing) error
	if cfg.Copy {
		copyCh, err := srcConn.Channel()
		if err != nil {
			return fmt.Errorf("open source copy channel: %w", err)
		}
		defer copyCh.Close()
		srcCopyFn = func(pub amqp.Publishing) error {
			return copyCh.PublishWithContext(ctx, "", cfg.Src.Queue, false, false, pub)
		}
	}

	msgs, err := srcCh.Consume(
		cfg.Src.Queue,
		"",    // auto consumer tag
		false, // manual ack
		false, // not exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case msg, ok := <-msgs:
			if !ok {
				return nil
			}

			// Reserve a processing slot when --count is set
			if cfg.Count > 0 {
				r := atomic.AddInt64(remaining, -1)
				if r < 0 {
					// Quota exhausted — put message back and stop this worker
					_ = msg.Nack(false, true)
					cancel()
					return nil
				}
			}

			pub := toPublishing(msg)

			// Publish to destination and wait for broker confirmation
			if err := dstCh.PublishWithContext(ctx, "", cfg.Dst.Queue, false, false, pub); err != nil {
				_ = msg.Nack(false, true)
				return fmt.Errorf("publish to dest: %w", err)
			}
			select {
			case confirm, ok := <-confirms:
				if !ok || !confirm.Ack {
					_ = msg.Nack(false, true)
					return fmt.Errorf("destination broker did not confirm message")
				}
			case <-ctx.Done():
				_ = msg.Nack(false, true)
				return nil
			}

			// Copy mode: republish message back to source queue
			if srcCopyFn != nil {
				if err := srcCopyFn(pub); err != nil {
					// Non-fatal: dest already received the message
					fmt.Fprintf(os.Stderr, "\nwarn: republish to source failed: %v\n", err)
				}
			}

			// Acknowledge source message (removes it in move mode, or confirms receipt in copy)
			if err := msg.Ack(false); err != nil {
				return fmt.Errorf("ack source message: %w", err)
			}

			atomic.AddInt64(processed, 1)
			atomic.StoreInt64(lastActivity, time.Now().UnixNano())
		}
	}
}

func printProgress(processed *int64, total int64, start time.Time) {
	n := atomic.LoadInt64(processed)
	elapsed := time.Since(start).Seconds()

	var rate float64
	if elapsed > 0 {
		rate = float64(n) / elapsed
	}

	const barW = 30
	filled := 0
	if total > 0 {
		filled = int(float64(n) / float64(total) * barW)
		if filled > barW {
			filled = barW
		}
	}

	bar := strings.Repeat("=", filled)
	if filled < barW {
		bar += ">"
		bar += strings.Repeat(" ", barW-filled-1)
	}

	fmt.Printf("\r[%s] %d/%d | %.0f msg/s | %.1fs   ",
		bar, n, total, rate, elapsed)
}

func main() {
	var (
		copyMode bool
		count    int64
		workers  int
		timeout  int
	)

	flag.BoolVar(&copyMode, "copy", false, "copy messages (keep originals in source queue)")
	flag.BoolVar(&copyMode, "c", false, "copy messages (shorthand for --copy)")
	flag.Int64Var(&count, "count", 0, "max number of messages to process (0 = all available)")
	flag.Int64Var(&count, "n", 0, "max messages (shorthand for --count)")
	flag.IntVar(&workers, "workers", 4, "number of parallel workers")
	flag.IntVar(&workers, "w", 4, "workers (shorthand for --workers)")
	flag.IntVar(&timeout, "timeout", 5, "seconds without new messages before stopping")
	flag.IntVar(&timeout, "t", 5, "timeout in seconds (shorthand for --timeout)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] SOURCE DEST\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  SOURCE / DEST:  [user:pass@]host[:port][/vhost]/queue\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Move all messages between queues on the same server\n")
		fmt.Fprintf(os.Stderr, "  %s guest:pass@localhost/fila1 guest:pass@localhost/fila2\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Copy to a different server with explicit vhost\n")
		fmt.Fprintf(os.Stderr, "  %s --copy guest:pass@rabbit1/meu-vhost/fila1 admin:pw@rabbit2/fila2\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Move at most 1000 messages using 8 workers\n")
		fmt.Fprintf(os.Stderr, "  %s -n 1000 -w 8 guest:pass@rabbit1/fila1 guest:pass@rabbit2/fila2\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	src, err := parseConnSpec(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in SOURCE: %v\n", err)
		os.Exit(1)
	}

	dst, err := parseConnSpec(flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in DEST: %v\n", err)
		os.Exit(1)
	}

	// Guard against moving a queue onto itself
	if src.Host == dst.Host && src.Port == dst.Port &&
		src.VHost == dst.VHost && src.Queue == dst.Queue {
		fmt.Fprintln(os.Stderr, "error: source and destination are the same queue")
		os.Exit(1)
	}

	if workers < 1 {
		workers = 1
	}

	cfg := &Config{
		Src:     src,
		Dst:     dst,
		Copy:    copyMode,
		Count:   count,
		Workers: workers,
		Timeout: time.Duration(timeout) * time.Second,
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
