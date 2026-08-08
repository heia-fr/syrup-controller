# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class PumpsBase(ABC):

    @abstractmethod
    async def cmd(self, message: int) -> int | None:
        pass
