// SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package pumps

import (
	"log/slog"
	"time"
)

type Pump struct {
	id          int
	running     bool
	stopTime    time.Time
	doCheckCup  bool
	pouringTime time.Duration
}

func (p *Pump) stop() {
	if !p.running {
		slog.Info("Pump is not running, ignoring command", "pump", p.id)
		return
	}
	p.running = false
	slog.Info("Stopping pump", "pump", p.id)
}

func (p *Pump) start() {
	if p.running {
		slog.Info("Pump is already running, ignoring command", "pump", p.id)
		return
	}
	slog.Info("Starting pump", "pump", p.id, "duration", p.pouringTime)
	p.running = true
	p.stopTime = time.Now().Add(p.pouringTime)
}

// ------

func (h *Handler) readSensor() error {
	val, err := h.cmdr.Send(kSensorBase)
	if err != nil {
		slog.Error("Failed to read sensor value", "error", err)
		h.sensor = 0
		return err
	}
	h.sensor = val
	return nil
}

func (h *Handler) isCupPresent(id int) bool {
	var check byte = kSensorBase | (1 << (kSensorShiftBase - id))
	return h.sensor&check == check
}

func (h *Handler) stopAll() {
	slog.Info("Stopping all pumps", "service", "pumps")
	for _, p := range h.pumps {
		p.stop()
	}
	h.rincing = false
}

func (h *Handler) start(id int) {
	slog.Info("Starting syrup", "service", "pumps", "syrup", id)
	if h.rincing {
		slog.Info("Purge is running, ignoring command", "pump", id)
		return
	}
	h.pumps[id].start()
}

func (h *Handler) rinse(duration time.Duration) {
	slog.Info("Rinse command received, starting rinse cycle", "service", "pumps")
	h.rincing = true
	h.rinceStopTime = time.Now().Add(duration)
}

func (h *Handler) softReset() {
	slog.Info("Soft Resetting pumps handler", "service", "pumps")
	h.stopAll()
	h.rincing = false
	h.state = 0x0
	_, err := h.cmdr.Send(0)
	if err != nil {
		slog.Error("Failed to send stop command during soft reset", "service", "pumps", "error", err)
	}
	val, err := h.cmdr.Send(kSoftReset)
	if err != nil {
		slog.Error("Failed to send soft reset command", "service", "pumps", "error", err)
	} else {
		slog.Info("Soft reset command sent successfully", "service", "pumps", "value", val)
	}
}

func (h *Handler) check(done chan byte) error {
	if h.rincing {
		if h.state != kRinse {
			slog.Info("Rincing started", "service", "pumps")
			_, err := h.cmdr.Send(kRinse)
			if err != nil {
				return err
			}
			h.state = kRinse
			return nil // start purging, no need to check pumps
		} else if time.Now().After(h.rinceStopTime) {
			slog.Info("Rincing done", "service", "pumps")
			h.stopAll()
		} else {
			return nil // Still purging, no need to check pumps
		}
	}

	now := time.Now()
	err := h.readSensor()
	if err != nil {
		return err
	}

	// Check if any pump should be stopped due to cup removal or duration elapsed
	for i, p := range h.pumps {
		if p.running {
			if !h.isCupPresent(i) && p.doCheckCup {
				slog.Warn("Cup not present, stopping pump", "service", "pumps", "pump", i)
				p.stop()
			} else if now.After(p.stopTime) {
				slog.Info("Pump duration elapsed, stopping pump", "service", "pumps", "pump", i)
				p.stop()
				done <- byte(i)
			}
		}
	}

	var cmd byte
	for i, p := range h.pumps {
		if p.running {
			cmd |= 1 << (kSyrupShiftBase - i) // Set bit for syrup i
			cmd |= 1 << (kWaterShiftBase - i) // Set bit for water i
		}
	}
	if cmd != h.state {
		slog.Info("Updating pump states", "service", "pumps", "state", cmd)
		_, err := h.cmdr.Send(cmd)
		if err != nil {
			slog.Error("Failed to send pump command", "command", cmd, "error", err)
			return err
		}
		for i, p := range h.pumps {
			if (h.state&(1<<(kSyrupShiftBase-i)) != 0) && !p.running {
				done <- byte(i)
			}
		}
		h.state = cmd
	}
	return nil
}
