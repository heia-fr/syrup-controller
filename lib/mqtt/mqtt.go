// SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package mqtt

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"syrup-controller/lib/pumps"

	"math"
	"net"
	"strings"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Handler struct {
	broker     string
	baseTopic  string
	qos        byte
	actionChan chan byte
	doneChan   chan byte
	client     mqtt.Client
}

func getUniqueId() (addr string) {
	interfaces, err := net.Interfaces()
	var mac net.HardwareAddr = nil
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && !bytes.Equal(i.HardwareAddr, nil) {
				mac = i.HardwareAddr
				break
			}
		}
	}
	if len(mac) == 0 {
		mac = make([]byte, 6)
		_, err = rand.Read(mac)
		if err != nil {
			slog.Error("Failed to generate random MAC address", "error", err)
			return "syrup-controller-unknown"
		}
	}

	const hexDigit = "0123456789abcdef"
	buf := make([]byte, 0, len(mac)*3-1)
	for _, b := range mac {
		buf = append(buf, hexDigit[b>>4])
		buf = append(buf, hexDigit[b&0xF])
	}
	return "syrup-controller-" + string(buf)

}

func NewHandler(
	broker string, baseTopic string, qos int,
	actionChan chan byte, doneChan chan byte) *Handler {
	return &Handler{
		broker:     broker,
		baseTopic:  strings.TrimRight(baseTopic, "/ "),
		qos:        byte(qos),
		actionChan: actionChan,
		doneChan:   doneChan,
	}
}

func (h *Handler) Run(ctx context.Context, wg *sync.WaitGroup) {

	mqtt.ERROR = NewMqttLogger(slog.LevelError, slog.Default())
	mqtt.CRITICAL = NewMqttLogger(slog.LevelError, slog.Default())
	mqtt.WARN = NewMqttLogger(slog.LevelWarn, slog.Default())
	mqtt.DEBUG = NewMqttLogger(slog.LevelDebug, slog.Default())

	id := getUniqueId()

	slog.Info("Starting MQTT client", "service", "mqtt", "clientId", id, "broker", h.broker)

	opts := mqtt.NewClientOptions().AddBroker(h.broker).SetClientID(id).SetCleanSession(true)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.ConnectTimeout = 10 * time.Second
	opts.SetConnectRetry(true)
	opts.SetMaxReconnectInterval(math.MaxInt64)

	buttonMessageHandler := func(client mqtt.Client, msg mqtt.Message) {
		payload := string(msg.Payload())
		h.actionChan <- byte(payload[0] - '0')
	}

	rinseHandler := func(client mqtt.Client, msg mqtt.Message) {
		h.actionChan <- pumps.CmdRinse
	}

	stopHandler := func(client mqtt.Client, msg mqtt.Message) {
		h.actionChan <- pumps.CmdStopAll
	}

	syrupHandler := func(client mqtt.Client, msg mqtt.Message) {
		payload := string(msg.Payload())
		id := byte(payload[0] - '0')
		if id >= 1 && id <= 3 {
			h.actionChan <- 0x00 + id
		} else {
			slog.Warn("Invalid syrup id received", "service", "mqtt", "payload", payload)
		}
	}

	softResetHandler := func(client mqtt.Client, msg mqtt.Message) {
		h.actionChan <- pumps.CmdSoftReset
	}

	opts.OnConnect = func(c mqtt.Client) {

		subscribe := func(topic string, handler mqtt.MessageHandler) {
			t := h.baseTopic + "/" + topic
			slog.Info("Subscribing", "service", "mqtt", "topic", t, "qos", h.qos)
			if token := c.Subscribe(t, h.qos, handler); token.Wait() && token.Error() != nil {
				panic("Failed to subscribe to topic " + t + ": " + token.Error().Error())
			}
		}
		slog.Info("Connected")
		subscribe("button", buttonMessageHandler)
		subscribe("rinse", rinseHandler)
		subscribe("stop", stopHandler)
		subscribe("start-syrup", syrupHandler)
		subscribe("soft-reset", softResetHandler)
	}

	h.client = mqtt.NewClient(opts)
	slog.Info("Connecting to MQTT broker.")
	if token := h.client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	slog.Info("MQTT client connected successfully.")

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping MQTT handler", "service", "mqtt")
			h.client.Disconnect(250)
			return
		case d := <-h.doneChan:
			slog.Info("Syrup done", "service", "mqtt", "syrup", d)
			h.client.Publish(h.baseTopic+"/done", h.qos, false, fmt.Sprintf("%d", int(d)+1))
		}
	}

}
