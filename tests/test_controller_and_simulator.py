# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

from datetime import timedelta

import pytest
from loguru import logger

from syrup_controller import RESET_COMMAND, SyrupController
from syrup_controller.pumps.base import PumpsBase
from syrup_controller.pumps.simulator import PumpsSimulator

POUR_MESSAGE = 0x09


class RecordingPumps(PumpsBase):
    def __init__(self):
        self.messages: list[int] = []

    async def cmd(self, message: int) -> int | None:
        self.messages.append(message)
        return None


@pytest.mark.asyncio
async def test_simulator_sets_pump_state() -> None:
    pumps = PumpsSimulator()

    reply = await pumps.cmd(POUR_MESSAGE)

    assert reply == 0
    assert pumps.pump_state == POUR_MESSAGE


@pytest.mark.asyncio
async def test_simulator_returns_current_cup_state() -> None:
    pumps = PumpsSimulator()
    pumps.remove_cup(1)

    reply = await pumps.cmd(0x40)

    assert reply is not None
    cups_bits = (reply >> 3) & 0x07
    assert cups_bits == pumps.cups_state


@pytest.mark.asyncio
async def test_controller_pour_and_stop_update_internal_state() -> None:
    controller = SyrupController(pumps=PumpsSimulator(), do_check_cup=False)
    controller.start_handler_if_needed = lambda: None

    await controller.pour(1, duration=timedelta(seconds=1))

    assert 1 in controller.pouring
    assert 1 in controller.stopping

    await controller.stop(1)

    assert 1 not in controller.pouring
    assert 1 not in controller.stopping


@pytest.mark.asyncio
async def test_controller_reset_sends_reset_and_clears_state() -> None:
    pumps = RecordingPumps()
    controller = SyrupController(pumps=pumps, do_check_cup=False)
    controller.start_handler_if_needed = lambda: None

    await controller.pour(0, duration=timedelta(seconds=1))
    assert controller.pouring

    await controller.reset()

    assert pumps.messages[-1] == RESET_COMMAND
    assert controller.pouring == set()
    assert controller.stopping == {}
    assert controller.rincing_started is None


def test_simulator_remove_cup_logs_message() -> None:
    pumps = PumpsSimulator()
    messages: list[str] = []
    sink_id = logger.add(messages.append, format="{message}")

    try:
        pumps.remove_cup(1)
    finally:
        logger.remove(sink_id)

    assert any("Cup 1 removed." in message for message in messages)
