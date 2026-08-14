# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from aiomqtt import Client as MQTTClient
from loguru import logger

from syrup_controller.pumps import PumpsBase

MAX_SYRUPS = 3
CMD_RESET = 0xC1  # Reset command for pumps
CMD_STOP_ALL = 0x00  # Stop all pumps command
CMD_CHECK_CUPS = 0x40  # Command to check cups state
CMD_RINSE = 0xC2  # Command to start cleaning mode

RINCE_STARTUP_TIME = timedelta(seconds=8)
TASK_SLEEP_TIME = 0.2  # seconds

RINSE_BUTTON = 8
RESET_BUTTON = 9


@dataclass
class SyrupController:
    pumps: PumpsBase
    mqtt_client: MQTTClient
    mqtt_base_topic: str
    do_check_cup: bool = True
    pour_duration: timedelta = timedelta(seconds=20)
    rinse_duration: timedelta = timedelta(seconds=30)
    pouring: set = field(default_factory=set)
    stopping: dict = field(default_factory=dict)
    handler = None
    rincing_started: datetime | None = None
    stop_rince: datetime | None = None

    async def _send_done(self, syrup: int):
        assert self.mqtt_client is not None
        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/done",
            f"{syrup}",
            qos=1,
        )

    async def _check_finish_rinse(self):
        now = datetime.now()
        if self.stop_rince is None:
            return
        elif now >= self.stop_rince:
            logger.info("Stopping rinse process")
            self.rincing_started = None
            self.stop_rince = None
            self.pouring.clear()
            self.stopping.clear()
            await self.pumps.cmd(CMD_STOP_ALL)  # Stop all pumps

    async def _check_cups(self):
        cups = await self.pumps.cmd(CMD_CHECK_CUPS)  # Get cups state
        # Check cups for syrups that are currently pouring
        for syrup in list(self.pouring):
            if not (cups & (1 << (6 - syrup))):
                logger.warning(f"Cup for syrup {syrup} is missing!")
                self.pouring.discard(syrup)
                if syrup in self.stopping:
                    del self.stopping[syrup]
                await self._send_done(syrup)

    async def _check_finish(self):
        now = datetime.now()
        for syrup, stop_time in list(self.stopping.items()):
            if now >= stop_time:
                logger.info(f"Stopping syrup {syrup}")
                self.pouring.discard(syrup)
                del self.stopping[syrup]
                await self._send_done(syrup)

    async def _task(self):
        prev_message = None
        logger.info("Starting Syrup Controller task")
        while True:

            if self.rincing_started is not None:
                await self._check_finish_rinse()
            else:
                if self.do_check_cup:
                    await self._check_cups()
                await self._check_finish()
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

            await asyncio.sleep(TASK_SLEEP_TIME)

    def _start_handler_if_needed(self):
        if self.handler is None:
            self.handler = asyncio.create_task(self._task())

    async def _pour(self, syrup: int, duration: timedelta):
        if self.rincing_started is not None:
            logger.warning("Cannot pour syrup while rinsing")
            await self._send_done(syrup)
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

        self._start_handler_if_needed()

    async def _stop(self, syrup: int):
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

        self._start_handler_if_needed()

    async def reset(self):
        logger.info("Resetting Syrup Controller")
        await self.pumps.cmd(CMD_RESET)  # Reset command
        try:
            for syrup in list(self.pouring):
                await self._send_done(syrup)
        except Exception as e:
            logger.error(f"Error sending done messages during reset: {e}")
        self.pouring.clear()
        self.stopping.clear()
        self.rincing_started = None
        if self.handler is not None:
            self.handler.cancel()
            self.handler = None

    async def _stop_all(self):
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
            await self.pumps.cmd(CMD_STOP_ALL)  # Stop all pumps
        try:
            for syrup in list(self.pouring):
                await self._send_done(syrup)
        except Exception as e:
            logger.error(f"Error sending done messages during stop all: {e}")
        self.pouring.clear()
        self.stopping.clear()
        self.rincing_started = None
        if self.handler is not None:
            self.handler.cancel()
            self.handler = None

    async def _rinse(self, duration: timedelta | None = timedelta(seconds=10)):
        if self.rincing_started is not None:
            logger.warning("Already rinsing, ignoring rinse command")
            return
        await self._stop_all()
        now = datetime.now()
        logger.info("Starting rinse process")
        self.rincing_started = now
        if duration is None:
            self.stop_rince = None
        else:
            self.stop_rince = now + duration
        await self.pumps.cmd(CMD_RINSE)  # Start cleaning mode
        self._start_handler_if_needed()

    @staticmethod
    def _parse_int_payload(payload: str, command_name: str) -> int | None:
        try:
            return int(payload)
        except ValueError:
            logger.error(f"Invalid payload for {command_name} command: {payload}")
            return None

    # Message handling
    async def _handle_button_command(self, payload: str):
        syrup = self._parse_int_payload(payload, "button")
        if syrup is None:
            return
        if 1 <= syrup <= MAX_SYRUPS:
            await self._pour(syrup, self.pour_duration)
        elif syrup == RINSE_BUTTON:
            await self._rinse(self.rinse_duration)
        elif syrup == RESET_BUTTON:
            await self.reset()
        else:
            logger.error(f"Invalid syrup number for button command: {syrup}")

    async def _handle_pour_command(self, payload: str):
        syrup = self._parse_int_payload(payload, "pour")
        if syrup is None:
            return
        await self._pour(syrup, self.pour_duration)

    async def _handle_stop_command(self, payload: str):
        syrup = self._parse_int_payload(payload, "stop")
        if syrup is None:
            return
        await self._stop(syrup)

    async def _handle_rinse_command(self, payload: str):
        sec = self._parse_int_payload(payload, "rinse")
        if sec is None:
            return
        await self._rinse(timedelta(seconds=sec) if sec > 0 else None)

    # MQTT message dispatching
    async def _dispatch_message(self, topic: str, payload: str):
        if topic.endswith("/button"):
            await self._handle_button_command(payload)
        elif topic.endswith("/pour"):
            await self._handle_pour_command(payload)
        elif topic.endswith("/reset"):
            await self.reset()
        elif topic.endswith("/stop-all"):
            await self._stop_all()
        elif topic.endswith("/stop"):
            await self._handle_stop_command(payload)
        elif topic.endswith("/rinse"):
            await self._handle_rinse_command(payload)

    async def _process_messages(self, client: MQTTClient):
        async for message in client.messages:
            topic = str(message.topic)
            payload = message.payload.decode()
            logger.debug(f"Received MQTT message: {topic} -> {payload}")
            await self._dispatch_message(topic, payload)

    # Main run loop
    async def run(self, stop_event: asyncio.Event | None = None):
        await self.pumps.connect()
        client = await self.mqtt_client.__aenter__()
        try:
            logger.info(f"Subscribing to topic {self.mqtt_base_topic}/#")
            await client.subscribe(f"{self.mqtt_base_topic}/#", qos=1)
            if stop_event is None:
                await self._process_messages(client)
            else:
                message_task = asyncio.create_task(self._process_messages(client))
                stop_task = asyncio.create_task(stop_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        (message_task, stop_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if message_task in done:
                        await message_task
                finally:
                    for task in (message_task, stop_task):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(
                        message_task, stop_task, return_exceptions=True
                    )
        finally:
            await self.mqtt_client.__aexit__(None, None, None)
