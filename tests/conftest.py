from __future__ import annotations

import os

import pytest

from missiva import Missiva
from tests.fakes import FakeBroker


@pytest.fixture(autouse=True)
def _clear_missiva_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop ``MISSIVA_*`` from the process env so settings tests stay isolated."""
    for key in list(os.environ):
        if key.startswith("MISSIVA_"):
            monkeypatch.delenv(key, raising=False)


def make_missiva(fake: FakeBroker) -> Missiva:
    """Return a ``Missiva`` whose broker port is ``fake``, not MQTT."""
    return Missiva._bind(fake)


@pytest.fixture
def fake_broker() -> FakeBroker:
    """Fresh in-memory broker for one test."""
    return FakeBroker()
