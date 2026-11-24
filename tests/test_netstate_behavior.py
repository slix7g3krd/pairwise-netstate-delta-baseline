import itertools
import json

import pytest

import gameplay.netstate_delta_baseline as mod


def snapshot(result):
    return {
        key: {"gen": value["gen"], "seq": value["seq"], "op": value["op"]}
        for key, value in result["entities"].items()
    }


# --- 1. 基线确认与重传 -------------------------------------------------------


def test_baseline_ack_retransmits_only_the_oldest_backlog():
    result = mod.baseline_ack(pending=[1, 2, 3, 4, 5], acked_version=2, max_unacked=2)
    assert result["acknowledged"] == [1, 2]
    assert result["retransmit"] == [3]
    assert result["keep"] == [4, 5]


def test_baseline_ack_honours_nack_and_force():
    assert mod.baseline_ack(pending=[3, 4, 5], acked_version=2, nack=[4])["retransmit"] == [4]
    assert mod.baseline_ack(pending=[1, 2], acked_version=0, force_retransmit=True)[
        "retransmit"
    ] == [1, 2]


def test_baseline_ack_does_not_retransmit_within_window():
    result = mod.baseline_ack(pending=[1, 2, 3], acked_version=0, max_unacked=8)
    assert result["retransmit"] == []
    assert result["pending_count"] == 3


def test_fallback_full_rebuild_only_when_baseline_lost():
    lost = mod.fallback(baseline=None)
    assert lost["full"] is True and lost["next_version"] == 1
    kept = mod.fallback({"baseline": {"version": 7}, "baseline_valid": True})
    assert kept["full"] is False and kept["next_version"] == 8


# --- 2. 优先级与配额 ---------------------------------------------------------


def test_critical_state_is_guaranteed_ahead_of_other_bands():
    result = mod.bandwidth_budget(
        budget=100, needs={"critical": 30, "high": 40, "normal": 50, "low": 80}
    )
    assert result["allocation"]["critical"] == 30
    assert result["critical_guaranteed"] is True


def test_tiny_budget_goes_entirely_to_critical():
    result = mod.bandwidth_budget(budget=3, needs={"critical": 5, "low": 9})
    assert result["allocation"]["critical"] == 3
    assert result["allocation"]["low"] == 0


def test_priority_quota_drops_low_band_packets_when_budget_is_short():
    result = mod.priority_quota(
        budget=10,
        packets=[
            {"id": "a", "priority": "critical", "size": 6, "seq": 1},
            {"id": "b", "priority": "high", "size": 5, "seq": 2},
            {"id": "c", "priority": "low", "size": 4, "seq": 3},
        ],
    )
    assert [packet["id"] for packet in result["selected"]] == ["a", "c"]
    assert [packet["id"] for packet in result["dropped"]] == ["b"]
    assert result["critical_dropped"] is False


# --- 3. ID 代际 --------------------------------------------------------------


def test_recreate_after_delete_bumps_generation():
    result = mod.id_generation({"id": "e1", "current_generation": 2, "deleted": True})
    assert result["generation"] == 3
    assert result["action"] == "recreate"


def test_delete_new_uses_generation_above_tombstone():
    result = mod.delete_new({"id": "e1", "active_generation": 3, "deleted": True})
    assert result["generation"] == 4
    assert result["conflict"] is False


def test_delete_new_flags_conflict_when_id_still_active():
    result = mod.delete_new({"id": "e1", "active_generation": 3})
    assert result["conflict"] is True
    assert result["reason"] == "id_still_active"


def test_same_generation_cannot_resurrect_after_delete():
    packet = {
        "seq": 3,
        "ops": [
            {"id": "e1", "gen": 2, "seq": 1, "op": "delete"},
            {"id": "e1", "gen": 2, "seq": 2, "attrs": {"hp": 1}},
        ],
    }
    result = mod.delta_apply({"entities": {}}, packet=packet, last_applied=2)
    assert snapshot(result)["e1"]["op"] == "delete"
    assert result["skipped"][0]["reason"] == "tombstoned"


# --- 4. 序号幂等与乱序 -------------------------------------------------------


def test_sequence_dedup_and_stale_rejection():
    assert mod.seq_idempotent({"seq": 5, "last_seq": 5})["reason"] == "duplicate"
    assert mod.seq_idempotent({"seq": 4, "last_seq": 5})["reason"] == "stale"
    assert mod.seq_idempotent({"seq": 6, "last_seq": 5})["apply"] is True


def test_reorder_guard_buffers_gap_and_drains_contiguous_run():
    gap = mod.reorder_guard({"seq": 3, "last_applied": 1, "buffered": [3, 4]})
    assert gap["accept"] is False and gap["drain"] == []
    drained = mod.reorder_guard({"seq": 2, "last_applied": 1, "buffered": [4]})
    assert drained["drain"] == [2]
    assert drained["last_applied"] == 2


def test_delta_apply_rejects_out_of_order_packet():
    stale = mod.delta_apply(
        {"entities": {}}, packet={"seq": 3, "ops": [{"id": "x", "seq": 1}]}, last_applied=5
    )
    assert stale["rejected"] is True and stale["reason"] == "stale"

    gap = mod.delta_apply(
        {"entities": {}}, packet={"seq": 5, "ops": [{"id": "x", "seq": 1}]}, last_applied=3
    )
    assert gap["rejected"] is True and gap["buffered"] == [5]


def test_delta_apply_is_order_independent():
    ops = [
        {"id": "a", "gen": 1, "seq": 2, "attrs": {"v": 2}},
        {"id": "a", "gen": 1, "seq": 5, "attrs": {"v": 5}},
        {"id": "a", "gen": 2, "seq": 1, "attrs": {"v": 20}},
        {"id": "b", "gen": 1, "seq": 1, "op": "delete"},
        {"id": "b", "gen": 2, "seq": 2, "attrs": {"v": 9}},
        {"id": "c", "gen": 1, "seq": 3, "op": "delete"},
    ]
    outcomes = {
        json.dumps(snapshot(mod.delta_apply({"entities": {}}, ops=list(perm))), sort_keys=True)
        for perm in itertools.permutations(ops)
    }
    assert len(outcomes) == 1


# --- 5. 重连基线协商 ---------------------------------------------------------


def test_reconnect_uses_delta_when_versions_match():
    result = mod.reconnect_base({"server_version": 10, "client_version": 8, "window": 5})
    assert result["mode"] == "delta"
    assert result["from_version"] == 9


@pytest.mark.parametrize(
    "state,reason",
    [
        ({"server_version": 10, "client_version": 8, "server_epoch": 2, "client_epoch": 1}, "epoch_mismatch"),
        ({"server_version": 10, "client_version": 11}, "client_ahead"),
        ({"server_version": 100, "client_version": 8, "window": 5}, "outside_window"),
        ({"server_version": 10, "client_version": 2, "compacted_before": 5}, "compacted"),
    ],
)
def test_reconnect_falls_back_to_full_on_baseline_mismatch(state, reason):
    result = mod.reconnect_base(state)
    assert result["mode"] == "full"
    assert result["reason"] == reason


# --- 6. 冲突合并 -------------------------------------------------------------


def test_merge_prefers_generation_then_tombstone_then_sequence():
    assert (
        mod.merge_rule(
            {"local": {"id": "a", "gen": 1, "seq": 9}, "remote": {"id": "a", "gen": 2, "seq": 3}}
        )["reason"]
        == "generation"
    )
    assert (
        mod.merge_rule(
            {
                "local": {"id": "a", "gen": 1, "seq": 9, "deleted": True},
                "remote": {"id": "a", "gen": 1, "seq": 10},
            }
        )["winner"]
        == "local"
    )
    assert (
        mod.merge_rule(
            {"local": {"id": "a", "gen": 1, "seq": 9}, "remote": {"id": "a", "gen": 1, "seq": 10}}
        )["reason"]
        == "sequence"
    )


def test_merge_is_symmetric_on_equal_sequence():
    left = {"id": "z", "gen": 1, "seq": 4, "attrs": {"v": 1}}
    right = {"id": "z", "gen": 1, "seq": 4, "attrs": {"v": 2}}
    forward = mod.merge_rule({"local": left, "remote": right})
    backward = mod.merge_rule({"local": right, "remote": left})
    assert forward["record"] == backward["record"]


# --- 辅助入口 ----------------------------------------------------------------


def test_validation_reports_structural_errors():
    result = mod.validation(
        {
            "packet": {
                "version": 3,
                "base_version": 5,
                "ops": [{"id": "a", "gen": 0, "seq": 1}, {"gen": 1}],
            }
        }
    )
    codes = {error["code"] for error in result["errors"]}
    assert result["valid"] is False
    assert {"base_ahead", "missing_id"} <= codes


def test_audit_is_stably_sorted():
    events = [
        {"kind": "apply", "id": "b", "gen": 0, "seq": 2},
        {"kind": "apply", "id": "a", "gen": 0, "seq": 1},
    ]
    ordered = mod.audit({"events": events})
    assert [event["id"] for event in ordered] == ["a", "b"]


def test_metrics_aggregates_counters_and_events():
    result = mod.metrics(
        {"counters": {"applied": 3}, "events": [{"kind": "stale"}, {"kind": "applied"}]}
    )
    assert result["applied"] == 4
    assert result["stale"] == 1
    assert result["total"] == 5


def test_determinism_digest_ignores_input_order_and_duplicates():
    ops = [{"id": "b", "seq": 2}, {"id": "a", "seq": 1}, {"id": "a", "seq": 1}]
    forward = mod.determinism({"ops": ops})
    backward = mod.determinism({"ops": list(reversed(ops))})
    assert forward["count"] == 2
    assert forward["digest"] == backward["digest"]
