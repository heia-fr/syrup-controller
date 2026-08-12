# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
import sys
from datetime import timedelta
from typing import NamedTuple
from urllib.parse import urlsplit

from aiomqtt import Client as MQTTClient
from loguru import logger
from typer import Option, Typer

from syrup_controller import SyrupController, context
from syrup_controller.pumps.simulator import PumpsSimulator
from syrup_controller.pumps.uart import PumpsUART

app = Typer()


class MqttConfig(NamedTuple):
    url: str
    base_topic: str
    username: str | None
    password: str | None


class UartConfig(NamedTuple):
    port: str | None
    baudrate: int


async def daemon(
    mqtt_config: MqttConfig,
    uart_config: UartConfig,
):
    if uart_config.port is None:
        logger.info("Starting Controller with Pumps Simulator")
        pumps = PumpsSimulator()
    else:
        logger.info("Starting Controller with Pumps UART")
        pumps = await PumpsUART.create(uart_config.port, uart_config.baudrate)

    o = urlsplit(mqtt_config.url)
    if o.scheme not in ("mqtt", "mqtts"):
        logger.error(f"Invalid MQTT URL: {mqtt_config.url}")
        return

    assert o.hostname is not None

    hostname = o.hostname
    port = o.port or (8883 if o.scheme == "mqtts" else 1883)

    logger.info(f"Connecting to MQTT broker at {hostname}:{port}")
    async with MQTTClient(
        hostname=hostname,
        port=port,
        username=mqtt_config.username,
        password=mqtt_config.password,
    ) as client:
        logger.debug(f"Subscribing to topic {mqtt_config.base_topic}/#")
        await client.subscribe(f"{mqtt_config.base_topic}/#", qos=1)
        controller = SyrupController(
            pumps=pumps,
            mqtt_client=client,
            mqtt_base_topic=mqtt_config.base_topic,
            do_check_cup=True,
        )
        logger.info(f"Base topic: {mqtt_config.base_topic}")
        logger.info("Starting controller")
        await controller.run()
    logger.info("Exiting controller")


@app.command()
def main(  # noqa
    debug: bool = Option(False, help="Enable debug logging", envvar="SYRUP_DEBUG"),
    quiet: bool = Option(False, help="Enable quiet logging", envvar="SYRUP_QUIET"),
    simulator: bool = Option(False, help="Use pumps simulator instead of UART"),
    uart_port: str | None = Option(
        "/dev/ttyAMA0", help="UART port for pumps", envvar="SYRUP_UART_PORT"
    ),
    uart_baudrate: int = Option(
        19200, help="UART baudrate for pumps", envvar="SYRUP_UART_BAUDRATE"
    ),
    mqtt_url: str = Option(
        "mqtt://mqtt.local:1883",
        help="MQTT broker URL",
        envvar="SYRUP_MQTT_URL",
    ),
    mqtt_base_topic: str = Option(
        "heiafr/ms/controller", help="MQTT base topic", envvar="SYRUP_MQTT_BASE_TOPIC"
    ),
    mqtt_username: str = Option(
        None, help="MQTT username", envvar="SYRUP_MQTT_USERNAME"
    ),
    mqtt_password: str = Option(
        None, help="MQTT password", envvar="SYRUP_MQTT_PASSWORD"
    ),
    pour_duration: int = Option(
        30, help="Duration of syrup pouring in seconds", envvar="SYRUP_POUR_DURATION"
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

    context["pour_duration"] = timedelta(seconds=pour_duration)

    if simulator:
        logger.info("Using pumps simulator")
        uart_port = None

    try:
        asyncio.run(
            daemon(
                MqttConfig(
                    url=mqtt_url,
                    base_topic=mqtt_base_topic,
                    username=mqtt_username,
                    password=mqtt_password,
                ),
                UartConfig(
                    port=uart_port,
                    baudrate=uart_baudrate,
                ),
            )
        )
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    app()
