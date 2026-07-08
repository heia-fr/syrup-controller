// SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package pumps

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.bug.st/serial"
)

type Commander interface {
	Send(cmd byte) (byte, error)
}

type SerialCommander struct {
	portName string
	mode     *serial.Mode
	port     serial.Port
}

func (c *SerialCommander) Open() error {
	var err error
	for {
		c.port, err = serial.Open(c.portName, c.mode)
		if err != nil {
			slog.Error("Failed to open serial port", "service", "cmdr", "port", c.portName, "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		return c.port.SetReadTimeout(commandTimeout)
	}
}

func NewSerialCommander(portName string) (*SerialCommander, error) {
	mode := &serial.Mode{
		BaudRate: 19200,
	}
	c := SerialCommander{portName: portName, mode: mode}
	err := c.Open()
	if err != nil {
		slog.Error("Failed to open serial port", "service", "cmdr", "port", portName, "error", err)
		return nil, err
	}
	return &c, nil
}

func (c *SerialCommander) reopen() error {
	err := c.port.Close()
	if err != nil {
		slog.Error("Failed to close serial port", "service", "cmdr", "port", c.portName, "error", err)
	}
	time.Sleep(1 * time.Second)
	return c.Open()
}

func (c *SerialCommander) Send(cmd byte) (byte, error) {
	var err error
	for range 5 {
		err = c.port.ResetInputBuffer()
		if err != nil {
			slog.Error("Failed to reset input buffer", "service", "cmdr", "error", err)
			err = c.reopen()
			if err != nil {
				slog.Error("Failed to reopen serial port", "service", "cmdr", "port", c.portName, "error", err)
			}
			continue
		}
		err = c.port.ResetOutputBuffer()
		if err != nil {
			slog.Error("Failed to reset output buffer", "service", "cmdr", "error", err)
			err = c.reopen()
			if err != nil {
				slog.Error("Failed to reopen serial port", "service", "cmdr", "port", c.portName, "error", err)
			}
			continue
		}

		slog.Debug("Sending", "service", "cmdr", "command", cmd)
		_, err = c.port.Write([]byte{cmd})
		if err != nil {
			slog.Error("Failed to write to serial port", "service", "cmdr", "command", cmd, "error", err)
			continue
		}

		buf := make([]byte, 1)
		_, err = c.port.Read(buf)
		if err != nil {
			slog.Error("Failed to read from serial port", "service", "cmdr", "command", cmd, "error", err)
			continue
		}
		slog.Debug("Received", "service", "cmdr", "response", buf[0])
		return buf[0], nil
	}
	return 0, err
}

const (
	commandTimeout = 500 * time.Millisecond
	rinseDuration  = 30 * time.Second
	checkPeriod    = 200 * time.Millisecond
)

const (
	CmdSyrup1    = 0x01
	CmdSyrup2    = 0x02
	CmdSyrup3    = 0x03
	CmdSoftReset = 0x08
	CmdRinse     = 0x09
	CmdStopAll   = 0x0F
)

const (
	kSoftReset       = 0xC1
	kRinse           = 0xC2
	kSensorBase      = 0x40
	kSensorShiftBase = 5
	kSyrupShiftBase  = 5
	kWaterShiftBase  = 2
)
const nOfPumps = 3

type Handler struct {
	cmdr          Commander
	state         byte
	sensor        byte
	rincing       bool
	rinceStopTime time.Time
	pumps         [nOfPumps]Pump
	actionChan    chan byte
	doneChan      chan byte
}

func NewHandler(cmdr Commander, bypassCupCheck bool, pouringTime time.Duration, actionChan chan byte, doneChan chan byte) *Handler {
	return &Handler{
		cmdr:    cmdr,
		state:   0x0,
		sensor:  0x0,
		rincing: false,
		pumps: [nOfPumps]Pump{
			{id: 0, doCheckCup: !bypassCupCheck, pouringTime: pouringTime},
			{id: 1, doCheckCup: !bypassCupCheck, pouringTime: pouringTime},
			{id: 2, doCheckCup: !bypassCupCheck, pouringTime: pouringTime},
		},
		actionChan: actionChan,
		doneChan:   doneChan,
	}
}

func (h *Handler) Run(ctx context.Context, wg *sync.WaitGroup) {
	slog.Info("Starting pumps handler", "service", "pumps")
	// Make sure the pumps are stopped at the beginning
	h.state = 0
	res, err := h.cmdr.Send(0)
	if err != nil {
		slog.Error("Failed to initialize pumps handler", "service", "pumps", "error", err)
		return
	}
	if res != 0 {
		slog.Warn("Unexpected response during initialization", "service", "pumps", "response", res)
	}

	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping pumps handler", "service", "pumps")
			h.stopAll()
			return
		// Handle incoming commands
		case cmd := <-h.actionChan:
			if cmd == CmdSoftReset {
				h.softReset()
			} else if cmd == CmdRinse {
				h.rinse(rinseDuration)
			} else if cmd == CmdStopAll {
				h.stopAll()
			} else if cmd >= CmdSyrup1 && cmd <= CmdSyrup3 {
				pump := cmd - CmdSyrup1
				h.start(int(pump))
			}
		case <-time.After(checkPeriod):
			// No command received, check pump states
		}
		err := h.check(h.doneChan)
		if err != nil {
			slog.Error("Error during pump check", "service", "pumps", "error", err)
		}
	}
}
