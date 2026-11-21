"""Ordering and cancellation of charger command decisions."""

import asyncio

import pytest
from ocpp_hub_core.prototype.command_processor import CommandPriority, CommandProcessor


# Intent: shutdown and Pause are ahead of queued ordinary commands, which keep their arrival order.
# Why: a stale or cancelled request must not stall or reorder the commands that are still eligible.
async def test_restrictions_overtake_pending_commands():
    commands = CommandProcessor()
    steps = []
    first = asyncio.create_task(commands.run(lambda: steps.append("first")), eager_start=True)
    retired = asyncio.create_task(commands.run(lambda: steps.append("retired")), eager_start=True)
    last = asyncio.create_task(commands.run(lambda: steps.append("last")), eager_start=True)
    pause = asyncio.create_task(
        commands.run(lambda: steps.append("pause"), priority=CommandPriority.RESTRICT), eager_start=True
    )
    shutdown = asyncio.create_task(
        commands.run(lambda: steps.append("shutdown"), priority=CommandPriority.SHUTDOWN), eager_start=True
    )
    assert steps == [
        "pause",
        "shutdown",
    ]  # Both restrictions commit before the loop can resume an older positive write.
    retired.cancel()
    with pytest.raises(asyncio.CancelledError):
        await retired
    await asyncio.gather(first, last, pause, shutdown)
    assert steps == ["pause", "shutdown", "first", "last"]


# Intent: a waiting owner and a refused decision both leave the processor available.
# Why: a slow cloud or charger reply must never make a Pause wait for an ordinary command to finish.
async def test_restriction_can_progress_while_owner_waits_for_a_reply():
    commands = CommandProcessor()
    waiting, reply = asyncio.Event(), asyncio.Event()

    # This owner has taken its turn and is waiting for evidence outside the processor.
    async def start() -> str:
        waiting.set()
        await reply.wait()
        return "reported"

    # A failure is reported to its own command and cannot strand other queued decisions.
    def refused() -> None:
        raise ValueError("refused")

    owner = commands.start(start)
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        with pytest.raises(ValueError, match="refused"):
            await commands.run(refused)
        assert await commands.run(lambda: "paused", priority=CommandPriority.RESTRICT) == "paused"
        assert not owner.done()
    finally:
        reply.set()
        assert await owner == "reported"


# Intent: incoming facts use the command order, and a nested state change stays in the same turn.
# Why: a reply must not let another decision run inside its journal transaction.
async def test_report_and_reply_decisions_share_the_command_order():
    commands = CommandProcessor()
    steps = []

    # The preceding command can apply an immediate nested decision within its transaction.
    def first():
        steps.append("command")
        commands.decide(lambda: steps.append("nested receipt"))
        steps.append("command finished")

    owner = asyncio.create_task(commands.run(first), eager_start=True)
    commands.decide(lambda: steps.append("pause"), priority=CommandPriority.RESTRICT)
    commands.decide(lambda: steps.append("report"))
    await owner
    assert steps == ["pause", "command", "nested receipt", "command finished", "report"]


# Intent: contention gives an already ready Pause a turn before an ordinary admission.
# Why: receiving a frame or preparing a send must not rush an older ordinary command ahead of Pause.
async def test_ordinary_admission_yields_to_ready_pause():
    commands = CommandProcessor()
    steps = []
    asyncio.get_running_loop().call_soon(
        lambda: commands.decide(lambda: steps.append("pause"), priority=CommandPriority.RESTRICT)
    )
    ordinary = asyncio.create_task(commands.run(lambda: steps.append("ordinary")), eager_start=True)
    await commands.yield_to_restrictions(CommandPriority.NORMAL)
    commands.decide(lambda: steps.append("admitted"))
    await ordinary
    assert steps == ["pause", "ordinary", "admitted"]
