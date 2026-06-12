"""
Unit tests for BlockManager.

These tests run without a GPU — block IDs are just integers.
"""

import pytest

from kvstream.memory.block_manager import BlockManager


@pytest.fixture
def bm():
    return BlockManager(num_gpu_blocks=16, num_cpu_blocks=32, block_size=4)


class TestAllocate:
    def test_basic_allocation(self, bm):
        ok = bm.allocate("seq1", num_tokens=4)
        assert ok is True
        assert bm.num_free_gpu_blocks() == 15  # 1 page used

    def test_multi_page_allocation(self, bm):
        ok = bm.allocate("seq1", num_tokens=9)  # ceil(9/4) = 3 pages
        assert ok is True
        assert bm.num_free_gpu_blocks() == 13

    def test_allocation_oom_returns_false(self, bm):
        # Fill all 16 blocks
        for i in range(16):
            bm.allocate(f"seq{i}", num_tokens=4)
        assert bm.num_free_gpu_blocks() == 0
        ok = bm.allocate("seq_overflow", num_tokens=1)
        assert ok is False

    def test_can_allocate_check(self, bm):
        assert bm.can_allocate(4) is True
        assert bm.can_allocate(64) is True  # 64/4 = 16 pages, exactly fits
        assert bm.can_allocate(65) is False  # 65/4 = 17 pages, exceeds pool


class TestAppendSlot:
    def test_append_within_block(self, bm):
        bm.allocate("seq1", num_tokens=1)
        block_id, slot = bm.append_slot("seq1")
        assert slot == 1  # second token in same block

    def test_append_crosses_block_boundary(self, bm):
        bm.allocate("seq1", num_tokens=4)  # fills block 0
        block_id_1, slot_1 = bm.append_slot("seq1")  # should get new block
        assert slot_1 == 0  # first slot in new block
        assert bm.num_free_gpu_blocks() == 14  # original 16 - 2

    def test_append_oom_raises(self, bm):
        # Fill all blocks
        for i in range(16):
            bm.allocate(f"seq{i}", num_tokens=4)
        bm.free("seq0")  # free 1 block
        bm.allocate("seq_new", num_tokens=4)  # re-use it
        # Now truly full
        with pytest.raises(MemoryError):
            bm.append_slot("seq1")


class TestFree:
    def test_free_returns_blocks(self, bm):
        bm.allocate("seq1", num_tokens=8)  # 2 pages
        assert bm.num_free_gpu_blocks() == 14
        bm.free("seq1")
        assert bm.num_free_gpu_blocks() == 16

    def test_free_unknown_seq_is_noop(self, bm):
        bm.free("nonexistent")  # should not raise
        assert bm.num_free_gpu_blocks() == 16


class TestFork:
    def test_fork_shares_blocks(self, bm):
        bm.allocate("parent", num_tokens=8)
        free_before = bm.num_free_gpu_blocks()
        bm.fork("parent", "child")
        # Fork doesn't allocate new blocks — it shares
        assert bm.num_free_gpu_blocks() == free_before

    def test_fork_increments_refcount(self, bm):
        bm.allocate("parent", num_tokens=4)
        bm.fork("parent", "child")
        block_id = bm._seq_states["parent"].logical_to_physical[0]
        assert bm._gpu_blocks[block_id].ref_count == 2

    def test_free_shared_block_waits_for_all_refs(self, bm):
        bm.allocate("parent", num_tokens=4)
        bm.fork("parent", "child")
        bm.free("parent")
        # Block still held by child
        assert bm.num_free_gpu_blocks() == 15
        bm.free("child")
        # Now released
        assert bm.num_free_gpu_blocks() == 16


class TestSwap:
    def test_swap_out_moves_to_cpu(self, bm):
        bm.allocate("seq1", num_tokens=4)
        gpu_free_before = bm.num_free_gpu_blocks()
        mapping = bm.swap_out("seq1")
        assert len(mapping) == 1
        assert bm.num_free_gpu_blocks() == gpu_free_before + 1

    def test_swap_in_returns_to_gpu(self, bm):
        bm.allocate("seq1", num_tokens=4)
        bm.swap_out("seq1")
        gpu_free_before = bm.num_free_gpu_blocks()
        bm.swap_in("seq1")
        assert bm.num_free_gpu_blocks() == gpu_free_before - 1


class TestUtilization:
    def test_utilization_empty(self, bm):
        assert bm.utilization() == 0.0

    def test_utilization_half_full(self, bm):
        for i in range(8):
            bm.allocate(f"seq{i}", num_tokens=4)
        assert bm.utilization() == pytest.approx(0.5)

    def test_utilization_full(self, bm):
        for i in range(16):
            bm.allocate(f"seq{i}", num_tokens=4)
        assert bm.utilization() == pytest.approx(1.0)
