// SPDX-FileCopyrightText: 2025 Jacques Supcik <jacques.supcik@hefr.ch>
//
// SPDX-License-Identifier: MIT

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Verbose        bool
	Debug          bool
	BypassCupCheck bool
	USBPort        string
	Broker         string
	BaseTopic      string
	PouringTime    int
)

var version = "dev"

var rootCmd = &cobra.Command{
	Use:   "syrup-controller",
	Short: "syrup-controller",

	Run: func(cmd *cobra.Command, args []string) {
		run()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
	rootCmd.Flags().BoolVarP(&Debug, "debug", "d", false, "debug output")
	rootCmd.Flags().StringVarP(&USBPort, "usb-port", "p", "/dev/ttyUSB0", "USB port to use")
	rootCmd.Flags().StringVarP(&Broker, "mqtt-broker", "b", "tcp://mqtt.local:1883", "The full URL of the MQTT broker to connect to")
	rootCmd.Flags().StringVarP(&BaseTopic, "mqtt-base-topic", "t", "syrup-controller", "Base topic to subscribe to")
	rootCmd.Flags().BoolVarP(&BypassCupCheck, "bypass-cup-check", "c", false, "Bypass cup presence check")
	rootCmd.Flags().IntVarP(&PouringTime, "pouring-time", "T", 20, "The time to pour syrup for (in seconds)")
	rootCmd.Version = version
}
