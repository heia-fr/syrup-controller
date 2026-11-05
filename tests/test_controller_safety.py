# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
#
# SPDX-License-Identifier: MIT

"""Regression tests: bad input must not stop the service, nor pumps be left running."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast, override

import pytest
from aiomqtt import Client as MQTTClient
from loguru import logger

from syrup_controller import (
    CMD_CHECK_CUPS,
    CMD_STOP_ALL,
    MAX_RINSE_DURATION_SECONDS,
    SyrupController,
)
from syrup_controller.pumps.base import PumpsBase
from syrup_controller.pumps.simulator import PumpsSimulator
from syrup_controller.pumps.uart import PumpsUART

pytestmark = pytest.mark.asyncio

LONG_POUR = timedelta(seconds=30)
SHORT_POUR = timedelta(seconds=0.05)
SLOW_COMMAND = 0.05  # seconds
WAIT_TIMEOUT = 2  # seconds
WAIT_POLL = 0.01  # seconds
ALL_CUPS = 0xFF
NO_CUPS = 0x00

SYRUP_1_WITH_WATER = 0b00100100
SYRUP_1_WATER_ONLY = 0b00100000
SYRUP_2_WITH_WATER = 0b00010010
SYRUP_1_AND_2_WITH_WATER = 0b00110110


class FakePumps(PumpsBase):
    def __init__(self, cups: int | None = ALL_CUPS, delay: float = 0) -> None:
        self.cups = cups
        self.delay = delay
        self.messages: list[int] = []

    @property
    def pump_commands(self) -> list[int]:
        return [m for m in self.messages if m != CMD_CHECK_CUPS]

    @override
    async def cmd(self, message: int) -> int | None:
        self.messages.append(message)
        await asyncio.sleep(self.delay)
        return self.cups if message == CMD_CHECK_CUPS else message

    @override
    async def connect(self) -> None:
        pass


class FakeMqtt:
    def __init__(self) -> None:
        self.done: list[str] = []
        self.on_publish: Callable[[str], Awaitable[None]] | None = None

    async def publish(self, _topic: str, payload: str, qos: int = 0) -> None:
        assert qos > 0
        self.done.append(payload)
        if self.on_publish is not None:
            await self.on_publish(payload)

    async def __aenter__(self) -> "FakeMqtt":
        return self

    async def __aexit__(self, *_args: object) -> None:
        pass

    async def subscribe(self, *_args: object, **_kwargs: object) -> None:
        pass

    @property
    def messages(self) -> Any:
        async def never_yields() -> Any:
            await asyncio.Event().wait()
            yield None

        return never_yields()


def make_controller(
    pumps: FakePumps | None = None, mqtt: FakeMqtt | None = None, **kwargs: Any
) -> SyrupController:
    return SyrupController(
        pumps=pumps or FakePumps(),
        mqtt_client=cast(MQTTClient, mqtt or FakeMqtt()),
        mqtt_base_topic="base",
        **kwargs,
    )


async def wait_for_task_end(controller: SyrupController) -> None:
    """Wait for the controller task to end, failing the test after WAIT_TIMEOUT."""
    async with asyncio.timeout(WAIT_TIMEOUT):
        while controller.handler is not None:
            await asyncio.sleep(WAIT_POLL)


async def wait_for_command(pumps: FakePumps, command: int) -> None:
    """Wait for the pumps to receive `command`, failing after WAIT_TIMEOUT."""
    async with asyncio.timeout(WAIT_TIMEOUT):
        while command not in pumps.messages:
            await asyncio.sleep(WAIT_POLL)


@pytest.mark.parametrize("topic", ["base/pour", "base/stop", "base/rinse"])
@pytest.mark.parametrize(
    "payload",
    [
        "1",
        "null",
        "[]",
        '"text"',
        "not json",
        "[" * 100_000,
        '{"syrup": ' + "1" * 5_000 + "}",
    ],
)
async def test_non_object_payload_is_ignored(topic: str, payload: str) -> None:
    controller = make_controller()

    await controller._dispatch_message(topic, payload)

    assert controller.pouring == {}
    assert controller.rinsing_started is None


@pytest.mark.parametrize(
    "payload",
    [
        '{"syrup": true}',
        '{"syrup": "1"}',
        '{"syrup": 1.0}',
        "{}",
        '{"syrup": 1, "check_cup": "false"}',
        '{"syrup": 1, "water_only": "false"}',
        '{"syrup": 1, "water_only": 1}',
    ],
)
async def test_invalid_pour_fields_are_rejected(payload: str) -> None:
    controller = make_controller()

    await controller._dispatch_message("base/pour", payload)

    assert controller.pouring == {}


@pytest.mark.parametrize(
    "duration",
    [
        "NaN",
        "Infinity",
        "-Infinity",
        "1e300",
        "true",
        '"30"',
        "null",
        str(MAX_RINSE_DURATION_SECONDS + 1),
    ],
)
async def test_invalid_rinse_duration_is_rejected(duration: str) -> None:
    controller = make_controller()

    await controller._dispatch_message("base/rinse", f'{{"duration": {duration}}}')

    assert controller.rinsing_started is None


@pytest.mark.parametrize(("duration", "endless"), [(30, False), (0, True), (-1, True)])
async def test_valid_rinse_duration_is_accepted(duration: int, endless: bool) -> None:
    controller = make_controller()

    await controller._dispatch_message("base/rinse", f'{{"duration": {duration}}}')

    assert controller.rinsing_started is not None
    assert (controller.stop_rinse is None) == endless
    await controller.reset()


async def test_non_utf8_message_is_skipped() -> None:
    def message(payload: bytes) -> SimpleNamespace:
        return SimpleNamespace(topic="base/pour", payload=payload)

    async def stream() -> Any:
        yield message(b"\xff\xfe")
        yield message(b'{"syrup": 2, "check_cup": false}')

    controller = make_controller(pour_duration=LONG_POUR)
    client = SimpleNamespace(messages=stream())

    await controller._process_messages(cast(MQTTClient, client))

    assert list(controller.pouring) == [2]
    await controller.reset()


@pytest.mark.parametrize(
    ("water_only", "expected"),
    [("false", SYRUP_1_WITH_WATER), ("true", SYRUP_1_WATER_ONLY)],
)
async def test_pump_command_bits(water_only: str, expected: int) -> None:
    pumps = FakePumps()
    controller = make_controller(pumps=pumps, pour_duration=LONG_POUR)

    await controller._dispatch_message(
        "base/pour", f'{{"syrup": 1, "check_cup": false, "water_only": {water_only}}}'
    )
    await wait_for_command(pumps, expected)

    await controller.reset()


async def test_client_can_disable_cup_check() -> None:
    pumps = FakePumps(cups=NO_CUPS)
    controller = make_controller(pumps=pumps, pour_duration=LONG_POUR)

    await controller._dispatch_message("base/pour", '{"syrup": 1, "check_cup": false}')
    await wait_for_command(pumps, SYRUP_1_WITH_WATER)

    assert 1 in controller.pouring
    await controller.reset()


async def test_client_can_enable_cup_check() -> None:
    pumps = FakePumps(cups=NO_CUPS)
    controller = make_controller(
        pumps=pumps, pour_duration=LONG_POUR, do_check_cup=False
    )

    await controller._dispatch_message("base/pour", '{"syrup": 1, "check_cup": true}')
    await wait_for_task_end(controller)

    assert controller.pouring == {}


async def test_failed_cup_query_stops_only_checked_pours() -> None:
    pumps = FakePumps(cups=None)
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)

    await controller._pour(1, LONG_POUR, check_cup=False)
    await controller._pour(2, LONG_POUR, check_cup=True)
    await wait_for_command(pumps, SYRUP_1_WITH_WATER)

    assert list(controller.pouring) == [1]
    assert mqtt.done == ["2"]
    await controller.reset()


async def test_failed_cup_query_turns_pumps_off() -> None:
    pumps = FakePumps(cups=None)
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)

    await controller._pour(1, LONG_POUR, check_cup=True)
    await wait_for_task_end(controller)

    assert pumps.pump_commands[-1] == CMD_STOP_ALL
    assert mqtt.done == ["1"]


async def test_stop_while_start_command_is_in_flight_turns_pumps_off() -> None:
    pumps = FakePumps(delay=SLOW_COMMAND)
    controller = make_controller(pumps=pumps)

    await controller._pour(1, LONG_POUR, check_cup=False)
    await wait_for_command(pumps, SYRUP_1_WITH_WATER)  # Reply still pending
    await controller._stop(1)
    await wait_for_task_end(controller)

    assert pumps.messages == [SYRUP_1_WITH_WATER, CMD_STOP_ALL]


async def test_stop_while_done_is_published_turns_pumps_off() -> None:
    pumps = FakePumps()
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)

    async def stop_syrup_2(payload: str) -> None:
        if payload == "1":
            await controller._stop(2)

    mqtt.on_publish = stop_syrup_2
    await controller._pour(1, SHORT_POUR, check_cup=False)
    await controller._pour(2, LONG_POUR, check_cup=False)
    await wait_for_task_end(controller)

    assert pumps.messages == [
        SYRUP_1_AND_2_WITH_WATER,
        SYRUP_2_WITH_WATER,
        CMD_STOP_ALL,
    ]
    assert mqtt.done == ["1"]


async def test_new_pour_during_done_publication_survives() -> None:
    mqtt = FakeMqtt()
    controller = make_controller(mqtt=mqtt)

    async def replace_syrup_2(payload: str) -> None:
        if payload == "1":
            await controller._stop(2)
            await controller._pour(2, LONG_POUR, check_cup=False)

    mqtt.on_publish = replace_syrup_2
    await controller._pour(1, SHORT_POUR, check_cup=False)
    await controller._pour(2, LONG_POUR, check_cup=False)
    async with asyncio.timeout(WAIT_TIMEOUT):
        while "1" not in mqtt.done:
            await asyncio.sleep(WAIT_POLL)
    await asyncio.sleep(SLOW_COMMAND)

    assert list(controller.pouring) == [2]
    assert mqtt.done == ["1"]
    await controller.reset()


@pytest.mark.parametrize(
    ("cups", "duration"), [(NO_CUPS, LONG_POUR), (ALL_CUPS, SHORT_POUR)]
)
async def test_pumps_stop_before_done_is_published(
    cups: int, duration: timedelta
) -> None:
    pumps = FakePumps(cups=cups)
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)
    commands_at_done: list[int] = []

    async def record_last_command(_payload: str) -> None:
        commands_at_done.append(pumps.pump_commands[-1])

    mqtt.on_publish = record_last_command
    await controller._pour(1, duration, check_cup=True)
    await wait_for_task_end(controller)

    assert commands_at_done == [CMD_STOP_ALL]
    assert mqtt.done == ["1"]


async def test_stop_all_during_pump_command_keeps_done() -> None:
    pumps = FakePumps(delay=SLOW_COMMAND)
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)

    await controller._pour(1, SHORT_POUR, check_cup=False)
    await wait_for_command(pumps, CMD_STOP_ALL)  # Reply still pending
    assert mqtt.done == []
    await controller._stop_all()

    assert mqtt.done == ["1"]
    assert controller.pending_done == []


async def test_task_failure_turns_pumps_off_and_allows_new_pours() -> None:
    pumps = FakePumps()
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)

    async def fail(_payload: str) -> None:
        raise ConnectionError("broker lost")

    mqtt.on_publish = fail
    await controller._pour(1, SHORT_POUR, check_cup=False)
    await wait_for_task_end(controller)

    assert pumps.pump_commands[-1] == CMD_STOP_ALL
    assert controller.pouring == {}

    mqtt.on_publish = None
    await controller._pour(1, LONG_POUR, check_cup=False)
    assert controller.handler is not None
    await controller.reset()


async def test_stop_all_is_the_last_command() -> None:
    pumps = FakePumps(delay=SLOW_COMMAND)
    controller = make_controller(pumps=pumps)

    await controller._pour(1, LONG_POUR, check_cup=False)
    await wait_for_command(pumps, SYRUP_1_WITH_WATER)
    await controller._stop_all()
    await asyncio.sleep(SLOW_COMMAND)

    assert pumps.messages[-1] == CMD_STOP_ALL
    assert controller.handler is None


@pytest.mark.parametrize("fail_run", [False, True])
async def test_run_exit_turns_pumps_off(fail_run: bool) -> None:
    pumps = FakePumps()
    mqtt = FakeMqtt()
    controller = make_controller(pumps=pumps, mqtt=mqtt)
    stop_event = asyncio.Event()

    async def fail_processing(client: MQTTClient) -> None:
        assert client is not None
        raise ConnectionError("broker lost")

    await controller._pour(1, LONG_POUR, check_cup=False)
    await wait_for_command(pumps, SYRUP_1_WITH_WATER)

    if fail_run:
        controller._process_messages = fail_processing
        with pytest.raises(ConnectionError):
            await controller.run(stop_event=stop_event)
    else:
        stop_event.set()
        await controller.run(stop_event=stop_event)

    assert pumps.messages[-1] == CMD_STOP_ALL
    assert controller.handler is None


async def test_uart_serializes_concurrent_commands() -> None:
    reader = asyncio.StreamReader()
    written: list[bytes] = []

    async def drain() -> None:
        await asyncio.sleep(0)

    writer = SimpleNamespace(write=written.append, drain=drain)
    pumps = PumpsUART(
        port="unused",
        baudrate=1,
        stream_reader=reader,
        stream_writer=cast(asyncio.StreamWriter, writer),
    )
    first_reply, second_reply = 0x41, 0x02

    first = asyncio.create_task(pumps.cmd(0x40))
    second = asyncio.create_task(pumps.cmd(0x00))
    await asyncio.sleep(WAIT_POLL)
    assert written == [b"\x40"]  # The second command waits for the first reply

    reader.feed_data(bytes([first_reply]))
    await asyncio.sleep(WAIT_POLL)
    reader.feed_data(bytes([second_reply]))

    assert await first == first_reply
    assert await second == second_reply
    assert written == [b"\x40", b"\x00"]


async def test_simulator_labels_water_and_syrup_bits() -> None:
    messages: list[str] = []
    sink_id = logger.add(messages.append, format="{message}")

    try:
        await PumpsSimulator().cmd(SYRUP_1_WATER_ONLY)
    finally:
        logger.remove(sink_id)

    assert any("starting water pump 1" in message for message in messages)
    assert not any("syrup pump" in message for message in messages)
