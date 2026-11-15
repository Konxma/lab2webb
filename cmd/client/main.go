package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", "", "target address (default 127.0.0.1:9000 TCP or :9001 UDP)")
	telemetry := flag.String("udp", "", "send this telemetry as one UDP datagram")
	flag.Parse()
	if err := run(*addr, *telemetry); err != nil {
		log.Fatal(err)
	}
}

func run(addr, telemetry string) error {
	if telemetry != "" {
		if addr == "" {
			addr = "127.0.0.1:9001"
		}
		conn, err := net.DialTimeout("udp", addr, 5*time.Second)
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, telemetry); err != nil {
			return err
		}
		fmt.Printf("Datagram sent to %s (%d bytes)\n", addr, len(telemetry))
		return nil
	}
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		_ = conn.(*net.TCPConn).CloseWrite()
	}()
	_, err = io.Copy(os.Stdout, conn)
	return err
}
