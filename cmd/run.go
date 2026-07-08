// SPDX-FileCopyrightText: 2025 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syrup-controller/lib/mqtt"
	"syrup-controller/lib/pumps"
	"syscall"
	"time"
)

const (
	qos = 1
)

func run() {
	var programLevel = new(slog.LevelVar) // Info by default
	slog.SetDefault(slog.New(
		slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})))
	if Debug {
		programLevel.Set(slog.LevelDebug)
	} else if !Verbose {
		programLevel.Set(slog.LevelWarn)
	}

	slog.Info("Starting Syrup Controller")

	actionChan := make(chan byte, 8)
	doneChan := make(chan byte)

	mqttHandler := mqtt.NewHandler(Broker, BaseTopic, qos, actionChan, doneChan)
	serialCommander, err := pumps.NewSerialCommander(USBPort)
	if err != nil {
		slog.Error("Failed to create serial commander", "error", err)
		return
	}
	pumpsHandler := pumps.NewHandler(
		serialCommander, BypassCupCheck,
		time.Duration(PouringTime)*time.Second,
		actionChan, doneChan)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	go mqttHandler.Run(ctx, &wg)
	wg.Add(1)

	go pumpsHandler.Run(ctx, &wg)
	wg.Add(1)

	slog.Info("OK")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	slog.Info("Signal received, shutting down")

	for i := range 3 {
		doneChan <- byte(i)
	}

	cancel()
	wg.Wait()

	_, err = serialCommander.Send(0)
	if err != nil {
		slog.Error("Failed to stop pumps", "error", err)
	}
	slog.Info("Bye")
}
