package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fireops-software/fireops-edge-alu2g-simulator/dal"
	"github.com/uoul/go-common/log"
)

const (
	TCP_TIMEOUT = 300 * time.Second
)

func main() {
	// Read command line flags
	logLvl := flag.String("logLvl", "INFO", "OFF, FATAL, ERROR, WARNING, INFO, DEBUG, TRACE")
	port := flag.Uint("p", 47000, "TCP-Port where simulated api will be exposed")
	maxOperations := flag.Int("maxOperations", 10, "Maximum of opations contained in a single messeage")
	flag.Parse()

	// Create Application context
	ctx, cancel := context.WithCancel(context.Background())

	// Create Logger
	logger := log.NewConsoleLogger(
		log.StringToLogLevel(
			*logLvl,
			log.INFO,
		),
	)
	// TestDataDao
	testDataDao := dal.NewPduDao()

	// Listen for TCP Connecitions
	go serveTcp(ctx, logger, uint16(*port), testDataDao, *maxOperations)

	// Terminate Application context
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-osSig
	cancel()
	logger.Infof("Shutting down...")
}

func serveTcp(ctx context.Context, logger log.ILogger, port uint16, testDataDao dal.ITestDataDao, maxOperations int) {
	tcp := net.ListenConfig{}
	ln, err := tcp.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("failed to bind on local tcp port - %v", err)
		return
	}
	logger.Infof("listening on %s ...", ln.Addr().String())
	for {
		// Accept incomming connection
		conn, err := ln.Accept()
		if err != nil {
			logger.Errorf("failed to accept incomming tcp connection - %v", err)
			continue
		}
		logger.Debugf("New incomming tcp connection accepted from %s", conn.RemoteAddr().String())
		// Handle Connection
		go handleTcpConnection(ctx, conn, logger, testDataDao, maxOperations)
	}
}

func handleTcpConnection(ctx context.Context, conn net.Conn, logger log.ILogger, testDataDao dal.ITestDataDao, maxOperations int) {
	defer conn.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Set Deadline
			conn.SetDeadline(
				time.Now().Add(TCP_TIMEOUT),
			)
			// Read - Request
			buffer := make([]byte, 256)
			n, err := conn.Read(buffer)
			if err != nil {
				if err != io.EOF {
					logger.Errorf("failed to read from tcp connection - %v", err)
				} else {
					logger.Debugf("client(%s) disconnected", conn.RemoteAddr().String())
				}
				return
			}
			for n >= len(buffer) {
				n, err = conn.Read(buffer)
				if err != nil {
					logger.Errorf("failed to read from tcp connection - %v", err)
					return
				}
			}
			logger.Debugf("New incomming request from %s", conn.RemoteAddr().String())
			// Write random testdata
			testData := testDataDao.GetRandomPdu(maxOperations)
			logger.Trace(testData)
			_, err = conn.Write([]byte(testData))
			if err != nil {
				logger.Errorf("failed to write to tcp connection - %v", err)
				return
			}
			logger.Debugf("Alerts has been written to %s", conn.RemoteAddr().String())
		}
	}
}
