"""Full-group events (``GROUP_MESSAGE_CREATE``) in the QQ Open Platform connector.

Once "receive all group messages" is enabled on the official bot, group events
arrive as ``GROUP_MESSAGE_CREATE`` instead of only ``GROUP_AT_MESSAGE_CREATE``
(field-compatible, plus a structured ``mentions`` array and ``<@openid>`` /
``<@all>`` in the body). The connector used to drop that event type on the
floor, so an @ to the bot in all-messages mode never reached the pipeline.

These tests target the vendored copy directly -- it is the file the patch
touches. Which copy runtime actually loads is ``connector_seam``'s call (host
first, vendored as fallback).

The rule under test is deliberately lopsided: a group message is delivered even
when it does not address the bot (the downstream attention gate decides whether
to speak); the *only* message dropped here is the bot's own.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from plugin.plugins.qq_auto_reply._vendor.connection_onebot import qq_open_plat

QQOpenPlatformConnection = qq_open_plat.QQOpenPlatformConnection


def _connection(self_id: str = "bot-self") -> QQOpenPlatformConnection:
    conn = QQOpenPlatformConnection.__new__(QQOpenPlatformConnection)
    conn._self_id = self_id
    return conn


def _payload(**overrides) -> dict:
    payload = {
        "id": "msg-full-1",
        "group_openid": "GROUP-OPENID-1",
        "author": {"member_openid": "user-1", "username": "Alice"},
        "content": "大家好",
    }
    payload.update(overrides)
    return payload


# A. A structured bot mention is recognized and the body is cleaned.
def test_mention_of_the_bot_sets_is_at_bot_and_cleans_the_body():
    msg = _connection()._convert_event(
        "GROUP_MESSAGE_CREATE",
        _payload(
            content="<@!bot-self> 在吗",
            mentions=[
                {"id": "bot-self", "member_openid": "bot-member", "bot": True, "nickname": "Bot"},
            ],
        ),
    )

    assert msg is not None
    assert msg["message_type"] == "group"
    assert msg["is_at_bot"] is True
    assert msg["content"] == "在吗"
    assert "<@" not in msg["content"]


# B. No mention of the bot is NOT a reason to drop the message.
def test_message_without_a_bot_mention_is_still_delivered():
    msg = _connection()._convert_event(
        "GROUP_MESSAGE_CREATE",
        _payload(
            content="<@user-2> 晚上吃啥",
            mentions=[{"id": "user-2", "member_openid": "user-2", "bot": False}],
        ),
    )

    assert msg is not None
    assert msg["message_type"] == "group"
    assert msg["is_at_bot"] is False
    assert msg["mentions_all"] is False


# C. @all addresses the bot, so it counts as is_at_bot as well.
def test_at_all_sets_mentions_all_and_counts_as_addressing_the_bot():
    msg = _connection()._convert_event(
        "GROUP_MESSAGE_CREATE",
        _payload(content="<@all> 开会了", mentions=[]),
    )

    assert msg is not None
    assert msg["mentions_all"] is True
    assert msg["is_at_bot"] is True
    assert msg["content"] == "开会了"


# D. The one and only drop path: the author is the bot itself.
def test_the_bot_s_own_message_is_dropped():
    msg = _connection()._convert_event(
        "GROUP_MESSAGE_CREATE",
        _payload(author={"member_openid": "bot-self", "username": "Bot"}),
    )

    assert msg is None


# E. No structured mentions at all: placeholder in the body only.
def test_missing_mentions_field_does_not_raise_and_cleans_placeholders():
    payload = _payload(content="<@ABCDEF123> 来看这个")
    payload.pop("mentions", None)

    msg = _connection()._convert_event("GROUP_MESSAGE_CREATE", payload)

    assert msg is not None
    assert msg["is_at_bot"] is False
    assert msg["mentions_all"] is False
    assert msg["content"] == "来看这个"


@pytest.mark.parametrize("mentions", [
    None,
    "oops",
    42,
    [None, "x", 7],
    [{"id": None, "member_openid": [], "bot": "true"}],
])
def test_malformed_mentions_never_raise(mentions):
    # A live protocol is not a schema: wrong types must degrade to "no mention
    # information", never to an exception (which _receive_loop reads as a
    # disconnect).
    msg = _connection()._convert_event(
        "GROUP_MESSAGE_CREATE",
        _payload(content="<@ABCDEF123> hi", mentions=mentions),
    )

    assert msg is not None
    assert msg["is_at_bot"] is False


def test_full_group_event_is_sampled_by_the_identity_probe():
    assert "GROUP_MESSAGE_CREATE" in qq_open_plat._IDENTITY_PROBE_EVENTS


class _ScriptedWS:
    """Feeds fixed frames, then stops the loop cleanly."""

    def __init__(self, connection, frames):
        self._connection = connection
        self._frames = list(frames)

    async def recv(self):
        if self._frames:
            return json.dumps(self._frames.pop(0))
        self._connection._closing = True
        return json.dumps({"op": 11})


@pytest.mark.asyncio
async def test_receive_loop_routes_full_group_events_into_the_queue():
    conn = QQOpenPlatformConnection.__new__(QQOpenPlatformConnection)
    conn.logger = None
    conn._emit_log = lambda level, msg: None
    conn._closing = False
    conn._last_seq = 0
    conn._self_id = "bot-self"
    conn._identity_probe = lambda: False
    conn._identity_probe_emitted = 0
    conn._message_queue = asyncio.Queue(maxsize=8)
    conn._ws = _ScriptedWS(conn, [{
        "op": 0,
        "s": 1,
        "t": "GROUP_MESSAGE_CREATE",
        "d": _payload(
            content="<@!bot-self> 在吗",
            mentions=[{"id": "bot-self", "bot": True}],
        ),
    }])

    await asyncio.wait_for(conn._receive_loop(), timeout=5.0)

    assert conn._message_queue.qsize() == 1
    msg = conn._message_queue.get_nowait()
    assert msg["message_type"] == "group"
    assert msg["is_at_bot"] is True
