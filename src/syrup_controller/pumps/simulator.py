# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

import asyncio
from dataclasses import dataclass, field

from loguru import logger

from syrup_controller.pumps.base import PumpsBase

BOOT_TIME = 2  # seconds
CMD_TIME = 0.1  # seconds

MAX_SYRUPS = 3

CMD_POURING = 0
CMD_GET_CUPS_STATE = 1
CMD_OTHER = 3

CMD_OTHER_RESET = 1
CMD_OTHER_CLEANING = 2


@dataclass
class PumpsSimulator(PumpsBase):
    pump_state: int = 0
    cups_state: int = 0x7
    prev_cups_state: int | None = None
    background_tasks: set = field(default_factory=set)

    async def cleaning(self):
        for i in range(1, 4):
            logger.info(f"Starting pump {i} for cleaning")
            self.pump_state |= 1 << (6 - i)
            await asyncio.sleep(2)

    async def cmd(self, message: int) -> int | None:  # noqa C901,PLR0912
        cmd = (message >> 6) & 0x03
        args = message & 0x3F

        reply = message & 0xC0

        if cmd == CMD_POURING:  # Set pump state
            if args == self.pump_state:
                logger.info(f"Pump state unchanged: {self.pump_state}")
            else:
                for i in range(3):
                    bit = 1 << (5 - i)
                    if (args & bit) != (self.pump_state & bit):
                        action = "starting" if args & bit else "stopping"
                        logger.info(f"{action} syrup pump {i+1}")
                for i in range(3):
                    bit = 1 << (2 - i)
                    if (args & bit) != (self.pump_state & bit):
                        action = "starting" if args & bit else "stopping"
                        logger.info(f"{action} water pump {i+1}")
            self.pump_state = args

        elif cmd == CMD_GET_CUPS_STATE:  # Get cups state
            states = [self.cups_state & (1 << (2 - i)) != 0 for i in range(3)]
            if self.prev_cups_state != self.cups_state:
                logger.info(f"Cups state changed: {self.cups_state:03b}")
                self.prev_cups_state = self.cups_state
            else:
                logger.debug(f"Getting cups state : {states}")
            reply |= (self.cups_state & 0x07) << 3
        elif cmd == CMD_OTHER:
            if args == CMD_OTHER_RESET:
                logger.info("RESET")
                await asyncio.sleep(BOOT_TIME)
                return None
            elif args == CMD_OTHER_CLEANING:
                logger.info("Starting Cleaning Mode")
                cleaning = asyncio.create_task(self.cleaning())
                self.background_tasks.add(cleaning)
                cleaning.add_done_callback(self.background_tasks.discard)

        else:
            logger.error(f"Unknown command: {cmd}")

        await asyncio.sleep(CMD_TIME)
        return reply

    def place_cup(self, cup_id: int):
        if cup_id < 1 or cup_id > MAX_SYRUPS:
            logger.error(f"Invalid cup ID: {cup_id}")
            return
        self.cups_state |= 1 << (MAX_SYRUPS - cup_id)
        logger.info(f"Cup {cup_id} placed.")

    def remove_cup(self, cup_id: int):
        if cup_id < 1 or cup_id > MAX_SYRUPS:
            logger.error(f"Invalid cup ID: {cup_id}")
            return
        self.cups_state &= ~(1 << (MAX_SYRUPS - cup_id))
        logger.info(f"Cup {cup_id} removed.")
