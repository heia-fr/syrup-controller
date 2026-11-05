# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
#
# SPDX-License-Identifier: MIT
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from aiomqtt import Client as MQTTClient
from loguru import logger

from syrup_controller.pumps import PumpsBase

MAX_SYRUPS = 3
CMD_RESET = 0xC1  # Reset command for pumps
CMD_STOP_ALL = 0x00  # Stop all pumps command
CMD_CHECK_CUPS = 0x40  # Command to check cups state
CMD_RINSE = 0xC2  # Command to start cleaning mode

RINSE_STARTUP_TIME = timedelta(seconds=8)
TASK_SLEEP_TIME = 0.2  # seconds

RINSE_BUTTON = 8
RESET_BUTTON = 9

MAX_RINSE_DURATION_SECONDS = 3600


def _parse_json_object(payload: str, command_name: str) -> dict[str, Any] | None:
    """Return the JSON object in `payload`, or None after logging if there is none."""
    try:
        request = json.loads(payload)
    except (ValueError, RecursionError) as e:
        logger.error(f"Invalid JSON in {command_name} command: {e}")
        return None
    if not isinstance(request, dict):
        logger.error(f"Expected a JSON object in {command_name} command: {payload}")
        return None
    return request


def _parse_syrup(request: dict[str, Any], command_name: str) -> int | None:
    """Return the integer `syrup` field, or None after logging if it is invalid."""
    syrup = request.get("syrup")
    if isinstance(syrup, bool) or not isinstance(syrup, int):
        logger.error(f"Invalid syrup number in {command_name} command: {syrup}")
        return None
    return syrup


def _parse_bool_option(
    request: dict[str, Any], name: str, default: bool
) -> bool | None:
    """Return the boolean option `name`, or None after logging if it is invalid."""
    value = request.get(name, default)
    if not isinstance(value, bool):
        logger.error(f"Invalid {name} in pour command: {value}")
        return None
    return value


@dataclass
class PourRequest:
    stop_time: datetime
    check_cup: bool = True
    water_only: bool = False


@dataclass
class SyrupController:
    pumps: PumpsBase
    mqtt_client: MQTTClient
    mqtt_base_topic: str
    do_check_cup: bool = True
    pour_duration: timedelta = timedelta(seconds=20)
    rinse_duration: timedelta = timedelta(seconds=30)
    pouring: dict[int, PourRequest] = field(default_factory=dict)
    # Finished pours whose done message is waiting for the pump command to be sent
    pending_done: list[int] = field(default_factory=list)

    handler = None
    rinsing_started: datetime | None = None
    stop_rinse: datetime | None = None

    async def _send_done(self, syrup: int):
        assert self.mqtt_client is not None
        await self.mqtt_client.publish(
            f"{self.mqtt_base_topic}/done",
            f"{syrup}",
            qos=1,
        )

    async def _check_finish_rinse(self):
        now = datetime.now()
        if self.stop_rinse is None:
            return
        elif now >= self.stop_rinse:
            logger.info("Stopping rinse process")
            self.rinsing_started = None
            self.stop_rinse = None
            self.pouring.clear()
            await self.pumps.cmd(CMD_STOP_ALL)  # Stop all pumps

    def _finish_pour(self, syrup: int):
        """End a pour; its done message follows once the pump command is sent."""
        del self.pouring[syrup]
        self.pending_done.append(syrup)

    async def _publish_pending_done(self):
        for syrup in list(self.pending_done):
            await self._send_done(syrup)
            self.pending_done.remove(syrup)

    async def _finish_all_pours(self):
        """Report every pour as done, tolerating MQTT failures, and clear state."""
        try:
            for syrup in [*self.pending_done, *self.pouring]:
                await self._send_done(syrup)
        except Exception as e:
            logger.error(f"Error sending done messages: {e}")
        self.pending_done.clear()
        self.pouring.clear()
        self.rinsing_started = None

    async def _check_cups(self):
        cups = await self.pumps.cmd(CMD_CHECK_CUPS)  # Get cups state
        if cups is None:
            logger.error("Failed to get cups state")
        # Check cups for syrups that are currently pouring. An unknown state counts
        # as a missing cup, so that a silent board cannot leave the pumps running.
        for syrup, request in list(self.pouring.items()):
            if not request.check_cup:
                continue
            if cups is None or not (cups & (1 << (6 - syrup))):
                logger.warning(f"Cup for syrup {syrup} is missing or unknown!")
                self._finish_pour(syrup)

    def _check_finish(self):
        now = datetime.now()
        for syrup, request in list(self.pouring.items()):
            if now >= request.stop_time:
                logger.info(f"Stopping syrup {syrup}")
                self._finish_pour(syrup)

    def _pump_command(self) -> int:
        """Return the pump command: bits 5-3 drive water, bits 2-0 drive syrup."""
        assert all(1 <= syrup <= MAX_SYRUPS for syrup in self.pouring)
        msg = 0
        for syrup, request in self.pouring.items():
            msg |= 1 << (6 - syrup)
            if not request.water_only:
                msg |= 1 << (3 - syrup)
        return msg

    async def _task(self):
        logger.info("Starting Syrup Controller task")
        try:
            await self._control_pumps()
        except Exception:
            logger.exception("Syrup Controller task failed, stopping all pumps")
            await self.pumps.cmd(CMD_STOP_ALL)
            await self._finish_all_pours()
        finally:
            self.handler = None

    async def _control_pumps(self):
        prev_message = None
        while True:
            if self.rinsing_started is not None:
                await self._check_finish_rinse()
            else:
                do_check_cup = any(
                    request.check_cup for request in self.pouring.values()
                )
                if do_check_cup:
                    await self._check_cups()
                self._check_finish()
                msg = self._pump_command()
                if msg != prev_message:
                    logger.info(f"Sending command: {msg:08b}")
                    await self.pumps.cmd(msg)
                    prev_message = msg
                # Only after the pump command: a slow broker must not delay the stop.
                await self._publish_pending_done()

            # A pour may end while its command is being sent, so also require that
            # the last command sent left the pumps off (None: nothing was sent).
            pumps_off = not prev_message
            if not self.pouring and self.rinsing_started is None and pumps_off:
                logger.info("No more syrups pouring, stopping task")
                return

            await asyncio.sleep(TASK_SLEEP_TIME)

    def _start_handler_if_needed(self):
        if self.handler is None:
            self.handler = asyncio.create_task(self._task())

    async def _cancel_handler(self):
        """Cancel the task and wait for it, so it cannot send any further command."""
        if self.handler is None:
            return
        handler = self.handler
        self.handler = None
        handler.cancel()
        # The outcome is deliberately ignored: _task logs its own failures.
        await asyncio.gather(handler, return_exceptions=True)

    async def _pour(
        self,
        syrup: int,
        duration: timedelta,
        water_only: bool = False,
        check_cup: bool = True,
    ):
        if self.rinsing_started is not None:
            logger.warning("Cannot pour syrup while rinsing")
            await self._send_done(syrup)
            return
        if syrup < 1 or syrup > MAX_SYRUPS:
            logger.error(f"Invalid syrup number: {syrup}")
            return
        if syrup in self.pouring:
            logger.warning(f"Syrup {syrup} is already pouring")
            return
        # We don't really start the pump here, we just add it to the list of pouring
        # syrups and let the task handle it
        request = PourRequest(
            check_cup=check_cup,
            water_only=water_only,
            stop_time=datetime.now() + duration,
        )
        self.pouring[syrup] = request
        self._start_handler_if_needed()

    async def _stop(self, syrup: int):
        if self.rinsing_started is not None:
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
        del self.pouring[syrup]
        self._start_handler_if_needed()

    async def reset(self):
        logger.info("Resetting Syrup Controller")
        await self._cancel_handler()
        await self.pumps.cmd(CMD_RESET)  # Reset command
        await self._finish_all_pours()

    async def _stop_all(self):
        await self._cancel_handler()
        now = datetime.now()
        if (
            self.rinsing_started is not None
            and now - self.rinsing_started < RINSE_STARTUP_TIME
        ):
            logger.warning(
                "Cannot stop all pumps while rinsing has just started. "
                "Resetting instead."
            )
            await self.reset()
        else:
            logger.info("Stopping all pumps")
            await self.pumps.cmd(CMD_STOP_ALL)  # Stop all pumps
        await self._finish_all_pours()

    async def _rinse(self, duration: timedelta | None = timedelta(seconds=10)):
        if self.rinsing_started is not None:
            logger.warning("Already rinsing, ignoring rinse command")
            return
        await self._stop_all()
        now = datetime.now()
        logger.info("Starting rinse process")
        self.rinsing_started = now
        if duration is None:
            self.stop_rinse = None
        else:
            self.stop_rinse = now + duration
        await self.pumps.cmd(CMD_RINSE)  # Start cleaning mode
        self._start_handler_if_needed()

    # Message handling
    async def _handle_button_command(self, payload: str):
        try:
            syrup = int(payload)
        except ValueError:
            logger.error(f"Invalid payload for button command: {payload}")
            return

        if 1 <= syrup <= MAX_SYRUPS:
            await self._pour(syrup, self.pour_duration, check_cup=self.do_check_cup)
        elif syrup == RINSE_BUTTON:
            await self._rinse(self.rinse_duration)
        elif syrup == RESET_BUTTON:
            await self.reset()
        else:
            logger.error(f"Invalid syrup number for button command: {syrup}")

    async def _handle_pour_command(self, payload: str):
        req = _parse_json_object(payload, "pour")
        if req is None:
            return
        syrup = _parse_syrup(req, "pour")
        do_check_cup = _parse_bool_option(req, "check_cup", self.do_check_cup)
        water_only = _parse_bool_option(req, "water_only", False)
        if syrup is None or do_check_cup is None or water_only is None:
            return
        await self._pour(
            syrup, self.pour_duration, check_cup=do_check_cup, water_only=water_only
        )

    async def _handle_stop_command(self, payload: str):
        req = _parse_json_object(payload, "stop")
        if req is None:
            return
        syrup = _parse_syrup(req, "stop")
        if syrup is None:
            return
        await self._stop(syrup)

    async def _handle_rinse_command(self, payload: str):
        req = _parse_json_object(payload, "rinse")
        if req is None:
            return
        sec = req.get("duration")
        # The range check also rejects NaN and the infinities.
        if (
            isinstance(sec, bool)
            or not isinstance(sec, (int, float))
            or not -MAX_RINSE_DURATION_SECONDS <= sec <= MAX_RINSE_DURATION_SECONDS
        ):
            logger.error(f"Invalid duration in rinse command: {sec}")
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
            try:
                payload = message.payload.decode()
            except UnicodeDecodeError as e:
                logger.error(f"Ignoring non UTF-8 MQTT message on {topic}: {e}")
                continue
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
            try:
                await self._stop_all()  # Never leave the pumps running on exit
            finally:
                await self.mqtt_client.__aexit__(None, None, None)
