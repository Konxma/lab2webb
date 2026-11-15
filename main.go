package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"dispatcher-gateway/internal/gateway"
)

func main() {
	tcpAddr := flag.String("tcp", "127.0.0.1:9000", "TCP commands address")
	udpAddr := flag.String("udp", "127.0.0.1:9001", "UDP telemetry address")
	flag.Parse()
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	if err := run(*tcpAddr, *udpAddr, logger); err != nil {
		logger.Fatal(err)
	}
}

func run(tcpAddr, udpAddr string, logger *log.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tcp, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		return err
	}
	defer tcp.Close()
	udp, err := net.ListenPacket("udp", udpAddr)
	if err != nil {
		return err
	}
	defer udp.Close()
	logger.Printf("[SERVER] TCP=%s UDP=%s", tcp.Addr(), udp.LocalAddr())
	return gateway.Serve(ctx, tcp, udp, logger)
}
