# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
import sys
from datetime import timedelta
from typing import Annotated
from urllib.parse import urlsplit

from aiomqtt import Client as MQTTClient
from loguru import logger
from typer import Option, Typer

from syrup_controller import SyrupController
from syrup_controller.pumps import PumpsSimulator, PumpsUART

app = Typer()


@app.command()
def main(  # noqa: PLR0913
    debug: bool = Option(False, help="Enable debug logging", envvar="SYRUP_DEBUG"),
    quiet: bool = Option(False, help="Enable quiet logging", envvar="SYRUP_QUIET"),
    simulator: bool = Option(False, help="Use pumps simulator instead of UART"),
    *,
    uart_port: Annotated[
        str,
        Option(
            help="UART port for pumps",
            envvar="SYRUP_UART_PORT",
            rich_help_panel="UART Settings",
        ),
    ] = "/dev/ttyAMA0",
    uart_baudrate: Annotated[
        int,
        Option(
            help="UART baudrate for pumps",
            envvar="SYRUP_UART_BAUDRATE",
            rich_help_panel="UART Settings",
        ),
    ] = 19200,
    mqtt_url: Annotated[
        str,
        Option(
            help="MQTT broker URL",
            envvar="SYRUP_MQTT_URL",
            rich_help_panel="MQTT Settings",
        ),
    ] = "mqtt://mqtt.local:1883",
    mqtt_base_topic: Annotated[
        str,
        Option(
            help="MQTT base topic",
            envvar="SYRUP_MQTT_BASE_TOPIC",
            rich_help_panel="MQTT Settings",
        ),
    ] = "heiafr/ms/controller",
    mqtt_username: Annotated[
        str | None,
        Option(
            help="MQTT username",
            envvar="SYRUP_MQTT_USERNAME",
            rich_help_panel="MQTT Settings",
        ),
    ] = None,
    mqtt_password: Annotated[
        str | None,
        Option(
            help="MQTT password",
            envvar="SYRUP_MQTT_PASSWORD",
            rich_help_panel="MQTT Settings",
        ),
    ] = None,
    pour_duration: int = Option(
        20, help="Duration of syrup pouring in seconds", envvar="SYRUP_POUR_DURATION"
    ),
    ignore_cups_check: bool = Option(
        False,
        help="Ignore cup presence check",
        envvar="SYRUP_IGNORE_CUPS_CHECK",
    ),
):
    """
    SYRUP-CONTROLLER

    This program is the gateway between MQTT messages and the pumps of th syrup
    machine. It is designed to be run on a Raspberry Pi connected to the pumps
    via UART.

    Copyright (c) 2026 Jacques Supcik, HEIA-FR
    """
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG")
    elif quiet:
        logger.add(sys.stderr, level="WARNING")
    else:
        logger.add(sys.stderr, level="INFO")

    if simulator:
        logger.info("Using pumps simulator")
        pumps = PumpsSimulator()
    else:
        pumps = PumpsUART(port=uart_port, baudrate=uart_baudrate)

    o = urlsplit(mqtt_url)
    if o.scheme not in ("mqtt", "mqtts"):
        logger.error(f"Invalid MQTT URL: {mqtt_url}")
        return

    if o.hostname is None:
        logger.error(f"Invalid MQTT URL: {mqtt_url}")
        return

    hostname = o.hostname
    port = o.port or (8883 if o.scheme == "mqtts" else 1883)

    logger.info(f"Connecting to MQTT broker at {hostname}:{port}")
    mqtt_client = MQTTClient(
        hostname=hostname,
        port=port,
        username=mqtt_username,
        password=mqtt_password,
    )

    controller = SyrupController(
        pumps=pumps,
        mqtt_client=mqtt_client,
        mqtt_base_topic=mqtt_base_topic,
        do_check_cup=not ignore_cups_check,
        pour_duration=timedelta(seconds=pour_duration),
    )

    try:
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    app()
