# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from aiomqtt import Client as MQTTClient
from loguru import logger

from syrup_controller.pumps.base import PumpsBase
from syrup_controller.pumps.simulator import PumpsSimulator

MAX_SYRUPS = 2
RESET_COMMAND = 0xC1  # Reset command for pumps


@dataclass
class SyrupController:
    pumps: PumpsBase
    mqtt_client: MQTTClient | None = None
    mqtt_base_topic: str | None = None
    do_check_cup: bool = True
    pouring: set = field(default_factory=set)
    stopping: dict = field(default_factory=dict)
    handler = None
    rincing: bool = False
    stop_rince: datetime | None = None

    async def check_finish_rinse(self):
        now = datetime.now()
        if self.stop_rince is None:
            return
        elif now >= self.stop_rince:
            logger.info("Stopping rinse process")
            self.rincing = False
            self.stop_rince = None
            self.pouring.clear()
            self.stopping.clear()
            await self.pumps.cmd(0x00)  # Stop all pumps

    async def check_cups(self):
        cups = await self.pumps.cmd(0x40)  # Get cups state
        for syrup in list(self.pouring):
            if not (cups & (1 << (5 - syrup))):
                logger.warning(f"Cup for syrup {syrup} is missing!")
                self.pouring.discard(syrup)
                if syrup in self.stopping:
                    del self.stopping[syrup]
                if self.mqtt_client and self.mqtt_base_topic:
                    await self.mqtt_client.publish(
                        f"{self.mqtt_base_topic}/{syrup}/cup", "missing"
                    )
                    await self.mqtt_client.publish(
                        f"{self.mqtt_base_topic}/{syrup}/status", "done"
                    )

    async def check_finish(self):
        now = datetime.now()
        for syrup, stop_time in list(self.stopping.items()):
            if now >= stop_time:
                logger.info(f"Stopping syrup {syrup}")
                self.pouring.discard(syrup)
                del self.stopping[syrup]
                if self.mqtt_client and self.mqtt_base_topic:
                    await self.mqtt_client.publish(
                        f"{self.mqtt_base_topic}/{syrup}/status", "done"
                    )

    async def task(self):
        prev_message = None
        logger.info("Starting Syrup Controller task")
        while True:

            if self.rincing:
                await self.check_finish_rinse()
            else:
                if self.do_check_cup:
                    await self.check_cups()
                await self.check_finish()
                msg = 0
                for i in self.pouring:
                    msg |= 1 << (5 - i) | 1 << (2 - i)
                if msg != prev_message:
                    logger.info(f"Sending command: {msg:08b}")
                    await self.pumps.cmd(msg)
                    prev_message = msg

            await asyncio.sleep(0.5)

    def start_handler_if_needed(self):
        if self.handler is None:
            self.handler = asyncio.create_task(self.task())

    async def pour(self, syrup: int, duration: timedelta = timedelta(seconds=5)):
        if self.rincing:
            logger.warning("Cannot pour syrup while rinsing")
            return
        if syrup < 0 or syrup > MAX_SYRUPS:
            logger.error(f"Invalid syrup number: {syrup}")
            return
        if syrup in self.pouring:
            logger.warning(f"Syrup {syrup} is already pouring")
            return
        self.pouring.add(syrup)
        self.stopping[syrup] = datetime.now() + duration

        self.start_handler_if_needed()

    async def stop(self, syrup: int):
        if self.rincing:
            logger.warning("Cannot stop syrup while rinsing")
            return
        if syrup < 0 or syrup > MAX_SYRUPS:
            logger.error(f"Invalid syrup number: {syrup}")
            return
        if syrup not in self.pouring:
            logger.warning(f"Syrup {syrup} is not pouring")
            return
        self.pouring.discard(syrup)
        if syrup in self.stopping:
            del self.stopping[syrup]

        self.start_handler_if_needed()

    async def reset(self):
        logger.info("Resetting Syrup Controller")
        await self.pumps.cmd(RESET_COMMAND)  # Reset command
        self.pouring.clear()
        self.stopping.clear()
        self.rincing = False
        if self.handler:
            self.handler.cancel()
            self.handler = None

    async def stop_all(self):
        logger.info("Stopping all pumps")
        self.rincing = False
        self.pouring.clear()
        self.stopping.clear()
        await self.pumps.cmd(0x00)  # Stop all pumps

    async def rinse(self, duration: timedelta | None = timedelta(seconds=10)):
        await self.stop_all()
        now = datetime.now()
        logger.info("Starting rinse process")
        self.rincing = True
        if duration is None:
            self.stop_rince = None
        else:
            self.stop_rince = now + duration
        await self.pumps.cmd(0xC2)  # Start cleaning mode
        self.start_handler_if_needed()

    async def run(  # noqa: C901, PLR0912
        self,
    ):
        if self.mqtt_client is None or self.mqtt_base_topic is None:
            if isinstance(self.pumps, PumpsSimulator):
                logger.warning(
                    "MQTT client or base topic not set. This is OK for testing only."
                )
            else:
                logger.error(
                    "MQTT client or base topic not set. Cannot run without MQTT."
                )
            return

        async for message in self.mqtt_client.messages:
            topic = str(message.topic)
            payload = message.payload.decode()
            logger.info(f"Received MQTT message: {topic} -> {payload}")

            if topic.endswith("/pour"):
                try:
                    syrup = int(payload)
                    await self.pour(syrup)
                except ValueError:
                    logger.error(f"Invalid payload for pour command: {payload}")

            elif topic.endswith("/reset"):
                await self.reset()

            elif topic.endswith("/stop-all"):
                await self.stop_all()

            elif topic.endswith("/stop"):
                try:
                    syrup = int(payload)
                    await self.stop(syrup)
                except ValueError:
                    logger.error(f"Invalid payload for stop command: {payload}")

            elif topic.endswith("/rinse"):
                await self.rinse(None)

            elif topic.endswith("/rinse-for"):
                try:
                    sec = int(payload)
                    await self.rinse(timedelta(seconds=sec))
                except ValueError:
                    logger.error(f"Invalid payload for rinse-for command: {payload}")
