// SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type MqttLogger struct {
	level  slog.Level
	logger *slog.Logger
}

func NewMqttLogger(level slog.Level, logger *slog.Logger) *MqttLogger {
	return &MqttLogger{
		level:  level,
		logger: logger,
	}
}

func (l *MqttLogger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	msg = strings.ReplaceAll(msg, "\n", " ")
	l.logger.Log(context.Background(), l.level, msg, "service", "mqtt")
}

func (l *MqttLogger) Println(v ...interface{}) {
	msg := fmt.Sprint(v...)
	l.logger.Log(context.Background(), l.level, msg, "service", "mqtt")
}
