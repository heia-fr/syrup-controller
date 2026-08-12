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

MAX_SYRUPS = 3
RESET_COMMAND = 0xC1  # Reset command for pumps
RINCE_STARTUP_TIME = timedelta(seconds=8)

RINSE_BUTTON = 8
RESET_BUTTON = 9

context: dict = {
    "pour_duration": timedelta(seconds=20),
    "rinse_duration": timedelta(seconds=30),
}


@dataclass
class SyrupController:
    pumps: PumpsBase
    mqtt_client: MQTTClient | None = None
    mqtt_base_topic: str | None = None
    do_check_cup: bool = True
    pouring: set = field(default_factory=set)
    stopping: dict = field(default_factory=dict)
    handler = None
    rincing_started: datetime | None = None
    stop_rince: datetime | None = None

    async def send_done(self, syrup: int):
        assert self.mqtt_client is not None
        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/done",
            f"{syrup}",
            qos=1,
        )

    async def check_finish_rinse(self):
        now = datetime.now()
        if self.stop_rince is None:
            return
        elif now >= self.stop_rince:
            logger.info("Stopping rinse process")
            self.rincing_started = None
            self.stop_rince = None
            self.pouring.clear()
            self.stopping.clear()
            await self.pumps.cmd(0x00)  # Stop all pumps

    async def check_cups(self):
        cups = await self.pumps.cmd(0x40)  # Get cups state
        # Check cups for syrups that are currently pouring
        for syrup in list(self.pouring):
            if not (cups & (1 << (6 - syrup))):
                logger.warning(f"Cup for syrup {syrup} is missing!")
                self.pouring.discard(syrup)
                if syrup in self.stopping:
                    del self.stopping[syrup]
                await self.send_done(syrup)

    async def check_finish(self):
        now = datetime.now()
        for syrup, stop_time in list(self.stopping.items()):
            if now >= stop_time:
                logger.info(f"Stopping syrup {syrup}")
                self.pouring.discard(syrup)
                del self.stopping[syrup]
                await self.send_done(syrup)

    async def task(self):
        prev_message = None
        logger.info("Starting Syrup Controller task")
        while True:

            if self.rincing_started is not None:
                await self.check_finish_rinse()
            else:
                if self.do_check_cup:
                    await self.check_cups()
                await self.check_finish()
                msg = 0
                for i in self.pouring:
                    msg |= 1 << (6 - i) | 1 << (3 - i)
                if msg != prev_message:
                    logger.info(f"Sending command: {msg:08b}")
                    await self.pumps.cmd(msg)
                    prev_message = msg

            if len(self.pouring) == 0 and self.rincing_started is None:
                logger.info("No more syrups pouring, stopping task")
                self.handler = None
                return

            await asyncio.sleep(0.5)

    def start_handler_if_needed(self):
        if self.handler is None:
            self.handler = asyncio.create_task(self.task())

    async def pour(self, syrup: int, duration: timedelta):
        if self.rincing_started is not None:
            logger.warning("Cannot pour syrup while rinsing")
            await self.send_done(syrup)
            return
        if syrup < 1 or syrup > MAX_SYRUPS:
            logger.error(f"Invalid syrup number: {syrup}")
            return
        if syrup in self.pouring:
            logger.warning(f"Syrup {syrup} is already pouring")
            return
        # We don't really start the punp here, we just add it to the list of pouring
        # syrups and let the task handle it
        self.pouring.add(syrup)
        self.stopping[syrup] = datetime.now() + duration

        self.start_handler_if_needed()

    async def stop(self, syrup: int):
        if self.rincing_started is not None:
            logger.warning("Cannot stop syrup while rinsing")
            return
        if syrup < 1 or syrup > MAX_SYRUPS:
            logger.error(f"Invalid syrup number: {syrup}")
            return
        if syrup not in self.pouring:
            logger.warning(f"Syrup {syrup} is not pouring")
            return
        # We don't really stop the pump here, we just remove it from the list of pouring
        # syrups and let the task handle it
        self.pouring.discard(syrup)
        if syrup in self.stopping:
            del self.stopping[syrup]

        self.start_handler_if_needed()

    async def reset(self):
        logger.info("Resetting Syrup Controller")
        await self.pumps.cmd(RESET_COMMAND)  # Reset command
        self.pouring.clear()
        self.stopping.clear()
        self.rincing_started = None
        if self.handler:
            self.handler.cancel()
            self.handler = None

    async def stop_all(self):
        now = datetime.now()
        if (
            self.rincing_started is not None
            and now - self.rincing_started < RINCE_STARTUP_TIME
        ):
            logger.warning(
                "Cannot stop all pumps while rinsing has just started. "
                "Resetting instead."
            )
            await self.reset()
        else:
            logger.info("Stopping all pumps")
            await self.pumps.cmd(0x00)  # Stop all pumps
        self.rincing_started = None
        self.pouring.clear()
        self.stopping.clear()

    async def rinse(self, duration: timedelta | None = timedelta(seconds=10)):
        if self.rincing_started is not None:
            logger.warning("Already rinsing, ignoring rinse command")
            return
        await self.stop_all()
        now = datetime.now()
        logger.info("Starting rinse process")
        self.rincing_started = now
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

            if topic.endswith("/button"):
                try:
                    syrup = int(payload)
                    if 1 <= syrup <= MAX_SYRUPS:
                        await self.pour(syrup, context["pour_duration"])
                    elif syrup == RINSE_BUTTON:
                        await self.rinse(context["rinse_duration"])
                    elif syrup == RESET_BUTTON:
                        await self.reset()
                    else:
                        logger.error(
                            f"Invalid syrup number for button command: {syrup}"
                        )
                except ValueError:
                    logger.error(f"Invalid payload for pour command: {payload}")

            elif topic.endswith("/pour"):
                try:
                    syrup = int(payload)
                    await self.pour(syrup, context["pour_duration"])
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
                try:
                    sec = int(payload)
                    await self.rinse(timedelta(seconds=sec) if sec > 0 else None)
                except ValueError:
                    logger.error(f"Invalid payload for rinse command: {payload}")
