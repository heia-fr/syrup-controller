# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
import sys
from urllib.parse import urlsplit

from aiomqtt import Client as MQTTClient
from loguru import logger
from typer import Option, Typer

from syrup_controller import SyrupController
from syrup_controller.pumps.simulator import PumpsSimulator
from syrup_controller.pumps.uart import PumpsUART

app = Typer()


async def daemon(
    pumps,
    mqtt_url,
    mqtt_base_topic,
    mqtt_username,
    mqtt_password,
):
    o = urlsplit(mqtt_url)
    if o.scheme not in ("mqtt", "mqtts"):
        logger.error(f"Invalid MQTT URL: {mqtt_url}")
        return

    logger.info(f"Connecting to MQTT broker at {o.hostname}:{o.port}")
    async with MQTTClient(
        hostname=o.hostname, port=o.port, username=mqtt_username, password=mqtt_password
    ) as client:
        await client.subscribe(f"{mqtt_base_topic}/#")
        controller = SyrupController(
            pumps=pumps,
            mqtt_client=client,
            mqtt_base_topic=mqtt_base_topic,
            do_check_cup=True,
        )
        await controller.run()


@app.command()
def main(  # noqa
    debug: bool = Option(False, help="Enable debug logging", envvar="SYRUP_DEBUG"),
    quiet: bool = Option(False, help="Enable quiet logging", envvar="SYRUP_QUIET"),
    simulator: bool = Option(False, help="Use pumps simulator instead of UART"),
    uart_port: str = Option(
        "/dev/ttyAMA0", help="UART port for pumps", envvar="SYRUP_UART_PORT"
    ),
    uart_baudrate: int = Option(
        9600, help="UART baudrate for pumps", envvar="SYRUP_UART_BAUDRATE"
    ),
    mqtt_url: str = Option(
        "mqtt://test.mosquitto.org:1883",
        help="MQTT broker URL",
        envvar="SYRUP_MQTT_URL",
    ),
    mqtt_base_topic: str = Option(
        "syrup", help="MQTT base topic", envvar="SYRUP_MQTT_BASE_TOPIC"
    ),
    mqtt_username: str = Option(
        None, help="MQTT username", envvar="SYRUP_MQTT_USERNAME"
    ),
    mqtt_password: str = Option(
        None, help="MQTT password", envvar="SYRUP_MQTT_PASSWORD"
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

    try:
        if simulator:
            logger.info("Starting Controller with Pumps Simulator")
            pumps = PumpsSimulator()
        else:
            logger.info("Starting Controller with Pumps UART")
            pumps = PumpsUART.create(uart_port, uart_baudrate)
        asyncio.run(
            daemon(pumps, mqtt_url, mqtt_base_topic, mqtt_username, mqtt_password)
        )
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    app()
