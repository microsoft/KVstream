"""
Unit tests for ContinuousBatchScheduler.
"""

import pytest

from kvstream.memory.block_manager import BlockManager
from kvstream.scheduler.continuous_batch import (
    ContinuousBatchScheduler,
    SeqStatus,
    SequenceGroup,
)


@pytest.fixture
def bm():
    return BlockManager(num_gpu_blocks=32, num_cpu_blocks=64, block_size=4)


@pytest.fixture
def scheduler(bm):
    return ContinuousBatchScheduler(
        block_manager=bm,
        max_batch_size=4,
        max_waiting_tokens=512,
        preemption_policy="swap",
        priority="fcfs",
    )


def make_seq(seq_id: str, prompt_len: int = 8, max_new: int = 64) -> SequenceGroup:
    return SequenceGroup(
        seq_id=seq_id,
        prompt_tokens=list(range(prompt_len)),
        max_new_tokens=max_new,
    )


class TestSchedule:
    @pytest.mark.asyncio
    async def test_waiting_seq_promoted_to_running(self, scheduler):
        await scheduler.add_request(make_seq("s1"))
        out = await scheduler.schedule()
        assert len(out.prefill) == 1
        assert out.prefill[0].seq_id == "s1"
        assert out.prefill[0].status == SeqStatus.RUNNING

    @pytest.mark.asyncio
    async def test_decode_vs_prefill_split(self, scheduler):
        seq = make_seq("s1")
        await scheduler.add_request(seq)
        await scheduler.schedule()  # promotes to running (prefill)

        # Simulate one token generated
        seq.output_tokens.append(42)

        await scheduler.add_request(make_seq("s2"))
        out = await scheduler.schedule()

        prefill_ids = [s.seq_id for s in out.prefill]
        decode_ids = [s.seq_id for s in out.decode]
        assert "s2" in prefill_ids
        assert "s1" in decode_ids

    @pytest.mark.asyncio
    async def test_max_batch_size_respected(self, scheduler):
        for i in range(10):
            await scheduler.add_request(make_seq(f"s{i}"))
        out = await scheduler.schedule()
        total_running = len(out.prefill) + len(out.decode)
        assert total_running <= 4  # max_batch_size

    @pytest.mark.asyncio
    async def test_preemption_on_oom(self):
        """With a tiny block pool, filling it should trigger swap-out."""
        tiny_bm = BlockManager(num_gpu_blocks=4, num_cpu_blocks=32, block_size=4)
        sched = ContinuousBatchScheduler(
            block_manager=tiny_bm,
            max_batch_size=8,
            preemption_policy="swap",
        )
        # 4 seqs × 4 tokens = exactly fills the pool
        for i in range(4):
            await sched.add_request(make_seq(f"s{i}", prompt_len=4))
        out1 = await sched.schedule()
        assert len(out1.prefill) == 4

        # Add one more — should preempt one running seq
        await sched.add_request(make_seq("s_new", prompt_len=4))
        out2 = await sched.schedule()
        assert len(out2.swapped_out) >= 1


class TestAbort:
    @pytest.mark.asyncio
    async def test_abort_waiting_seq(self, scheduler):
        await scheduler.add_request(make_seq("s1"))
        await scheduler.abort_request("s1")
        assert len(scheduler.waiting) == 0

    @pytest.mark.asyncio
    async def test_abort_running_seq(self, scheduler):
        await scheduler.add_request(make_seq("s1"))
        await scheduler.schedule()
        await scheduler.abort_request("s1")
        assert len(scheduler.running) == 0


class TestStats:
    @pytest.mark.asyncio
    async def test_stats_reflect_state(self, scheduler):
        for i in range(3):
            await scheduler.add_request(make_seq(f"s{i}"))
        stats = scheduler.stats()
        assert stats["waiting"] == 3
        assert stats["running"] == 0

        await scheduler.schedule()
        stats = scheduler.stats()
        assert stats["running"] == 3
        assert stats["waiting"] == 0
