# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
from dataclasses import dataclass
from typing import override

import serial_asyncio
from loguru import logger

from syrup_controller.pumps.base import PumpsBase

CMD_TIMEOUT = 1  # seconds
RESET_COMMAND = 0xC0  # Reset command for pumps


@dataclass
class PumpsUART(PumpsBase):
    port: str = "/dev/ttyAMA0"
    baudrate: int = 19200
    stream_reader: asyncio.StreamReader | None = None
    stream_writer: asyncio.StreamWriter | None = None

    @override
    async def cmd(self, message: int) -> int | None:
        if self.stream_writer is None or self.stream_reader is None:
            raise RuntimeError("UART connection not established")

        await self.stream_writer.drain()
        self.stream_writer.write(bytes([message]))
        await self.stream_writer.drain()

        if message & 0xC0 == RESET_COMMAND:  # Reset command, no reply expected
            return None

        try:
            async with asyncio.timeout(CMD_TIMEOUT):
                result = await self.stream_reader.read(1)
        except TimeoutError:
            logger.warning("Command timed out")
            return None

        if len(result) == 0:
            logger.warning("No reply received")
            return None
        return result[0]

    @override
    async def connect(self):
        logger.info(f"Opening UART connection on {self.port} at {self.baudrate} baud")
        self.stream_reader, self.stream_writer = (
            await serial_asyncio.open_serial_connection(
                url=self.port, baudrate=self.baudrate
            )
        )
        logger.info("UART connection established.")
