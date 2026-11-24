"""状态增量与基线同步的核心实现。

题目中的六类问题被拆成可独立验证的策略入口：

1. 基线确认与重传  ``baseline_ack`` / ``fallback``
2. 优先级与配额    ``bandwidth_budget`` / ``priority_quota``
3. 对象 ID 代际    ``id_generation`` / ``delete_new``
4. 序号幂等与乱序  ``seq_idempotent`` / ``reorder_guard``
5. 重连基线协商    ``reconnect_base``
6. 冲突合并规则    ``merge_rule`` / ``delta_apply``

另附 ``validation``、``audit``、``metrics``、``determinism`` 四个辅助入口。

所有入口都接受一个位置参数（通常是状态映射）以及可选关键字参数，
返回非 ``None`` 的普通 ``dict`` / ``list``，可直接交给网络层序列化。
当传入的不是映射时，它会被当作该函数的自然标量（例如版本、序号或预算）。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

__all__ = [
    "baseline_ack",
    "priority_quota",
    "id_generation",
    "seq_idempotent",
    "reconnect_base",
    "merge_rule",
    "delta_apply",
    "validation",
    "audit",
    "fallback",
    "bandwidth_budget",
    "metrics",
    "delete_new",
    "reorder_guard",
    "determinism",
]

OP_UPSERT = "upsert"
OP_DELETE = "delete"

PRIORITY_LABELS = {0: "low", 1: "normal", 2: "high", 3: "critical"}
PRIORITY_NAMES = {label: band for band, label in PRIORITY_LABELS.items()}

# 非关键优先级之间的公平权重；critical 总是先于配额分配。
_BAND_WEIGHTS = {2: 4, 1: 2, 0: 1}

_COUNTER_KEYS = (
    "applied",
    "stale",
    "duplicate",
    "conflict",
    "tombstone",
    "invalid",
    "quota_dropped",
    "retransmit",
    "full_rebuild",
)


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------


def _arguments(args, kwargs):
    """归一化入口参数：位置参数是状态映射或标量，关键字参数覆盖之。"""

    state: dict = {}
    if args:
        first = args[0]
        if isinstance(first, Mapping):
            state.update(first)
        else:
            state["value"] = first
    state.update(kwargs)
    return state


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _maybe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _priority(value):
    if isinstance(value, str):
        name = value.strip().lower()
        if name in PRIORITY_NAMES:
            return PRIORITY_NAMES[name]
    return max(0, min(3, _as_int(value, 1)))


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _digest(value):
    if value is None:
        raw = ""
    elif isinstance(value, str):
        raw = value
    else:
        raw = _canonical(value)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _zero_counters():
    return {key: 0 for key in _COUNTER_KEYS}


def _normalise_packet(raw):
    """把任意包描述归一化为 id/gen/seq/priority/size 五元组。"""

    record = dict(raw) if isinstance(raw, Mapping) else {"id": raw}
    record["id"] = record.get("id")
    record["gen"] = _as_int(record.get("gen", record.get("generation", 0)), 0)
    record["seq"] = _as_int(record.get("seq", 0), 0)
    record["priority"] = _priority(record.get("priority", 1))
    size = record.get("size")
    if size is None:
        payload = record.get("attrs")
        size = len(_canonical(payload)) if payload is not None else 1
    record["size"] = max(0, _as_int(size, 1))
    return record


def _record(raw):
    """把实体记录归一化；``deleted`` 与 ``op`` 保持互相一致。"""

    record = dict(raw) if isinstance(raw, Mapping) else {"id": raw}
    record["id"] = record.get("id")
    record["gen"] = _as_int(record.get("gen", record.get("generation", 0)), 0)
    record["seq"] = _as_int(record.get("seq", 0), 0)
    record["priority"] = _priority(record.get("priority", 1))
    op = record.get("op")
    deleted = bool(record.get("deleted", False)) or op == OP_DELETE
    record["deleted"] = deleted
    record["op"] = OP_DELETE if deleted and op is None else str(op if op is not None else OP_UPSERT)
    record["deleted"] = record["deleted"] or record["op"] == OP_DELETE
    return record


def _reorder(state):
    """按流序号决定接受、缓冲还是丢弃，返回连续可排空的范围。"""

    seq = _as_int(state.get("seq", state.get("value", 0)))
    last = _as_int(state.get("last_applied", 0))
    pool = set()
    raw_buffered = state.get("buffered")
    if isinstance(raw_buffered, (list, tuple, set, frozenset)):
        for item in raw_buffered:
            candidate = _maybe_int(item)
            if candidate is not None and candidate >= 0:
                pool.add(candidate)

    if seq <= last:
        return {
            "accept": False,
            "reason": "duplicate" if seq == last else "stale",
            "buffered": sorted(pool),
            "drain": [],
            "last_applied": last,
        }

    if seq == last + 1:
        drain = [seq]
        last = seq
        pool.discard(seq)
        while last + 1 in pool:
            last += 1
            drain.append(last)
            pool.discard(last)
        return {
            "accept": True,
            "reason": "in_order",
            "buffered": sorted(pool),
            "drain": drain,
            "last_applied": last,
        }

    pool.add(seq)
    return {
        "accept": False,
        "reason": "gap",
        "buffered": sorted(pool),
        "drain": [],
        "last_applied": last,
    }


# ---------------------------------------------------------------------------
# 1. 基线确认与重传
# ---------------------------------------------------------------------------


def baseline_ack(*args, **kwargs):
    """确认客户端已落盘的基线版本，并挑出需要重传的版本。

    客户端回报 ``acked_version`` 后，所有不高于它的待确认基线都被移除；
    超过 ``max_unacked`` 窗口的最旧基线、以及客户端显式 NACK 的版本会被重传。
    重传是选择性的，不会因为一次丢包就退化为全量。
    """

    state = _arguments(args, kwargs)
    acked = _as_int(state.get("acked_version", state.get("ack", state.get("value", 0))), 0)
    max_unacked = max(1, _as_int(state.get("max_unacked", 8), 8))

    raw_pending = state.get("pending", state.get("sent", []))
    pending = raw_pending if isinstance(raw_pending, (list, tuple, set)) else []
    versions = []
    for item in pending:
        if isinstance(item, Mapping):
            version = _as_int(item.get("version", item.get("value", -1)), -1)
        else:
            version = _maybe_int(item)
            version = -1 if version is None else version
        if version >= 0:
            versions.append(version)
    versions = sorted(set(versions))

    acknowledged = [version for version in versions if version <= acked]
    unacked = [version for version in versions if version > acked]

    retransmit = []
    if state.get("force_retransmit"):
        retransmit = list(unacked)
    elif len(unacked) > max_unacked:
        # 只重传最旧的积压部分，保留最近发送的窗口，避免重传风暴。
        retransmit = unacked[: len(unacked) - max_unacked]

    raw_nack = state.get("nack", [])
    if isinstance(raw_nack, (list, tuple, set)):
        for item in raw_nack:
            version = _maybe_int(item)
            if version is not None and version >= 0:
                retransmit.append(version)

    retransmit = sorted(set(retransmit))
    keep = [version for version in unacked if version not in retransmit]

    return {
        "acked_version": acked,
        "acknowledged": acknowledged,
        "retransmit": retransmit,
        "keep": keep,
        "pending_count": len(keep),
        "next_version": acked + 1,
        "window": max_unacked,
    }


def fallback(*args, **kwargs):
    """基线不可用时退化为全量重建；可用时保持增量路径。"""

    state = _arguments(args, kwargs)
    baseline = state.get("baseline")
    baseline_version = None
    if isinstance(baseline, Mapping):
        baseline_version = _maybe_int(baseline.get("version"))
    if baseline_version is None:
        baseline_version = _maybe_int(state.get("baseline_version"))

    valid = bool(state.get("baseline_valid", baseline is not None))
    reason = state.get("reason")

    if baseline is not None and valid:
        return {
            "mode": "delta",
            "full": False,
            "reason": reason or "baseline_available",
            "baseline_version": baseline_version,
            "reset_pending": False,
            "next_version": (baseline_version or 0) + 1,
        }

    return {
        "mode": "full_rebuild",
        "full": True,
        "reason": reason or "baseline_lost",
        "baseline_version": None,
        "reset_pending": True,
        "snapshot_required": True,
        "next_version": 1,
        "metrics_key": "full_rebuild",
    }


# ---------------------------------------------------------------------------
# 2. 优先级与配额
# ---------------------------------------------------------------------------


def _allocate(budget, needs):
    """先保证 critical，再在其余优先级之间做加权公平分配。"""

    budget = max(0, _as_int(budget, 0))
    need = {band: max(0, _as_int(needs.get(band, 0), 0)) for band in PRIORITY_LABELS}
    allocation = {band: 0 for band in PRIORITY_LABELS}
    remaining = budget

    # 关键状态拥有绝对优先权：预算再小也不会被低优先级挤掉。
    allocation[3] = min(need[3], remaining)
    remaining -= allocation[3]

    active = [band for band in (2, 1, 0) if need[band] > 0]
    while remaining > 0 and active:
        total_weight = sum(_BAND_WEIGHTS[band] for band in active)
        shares = {
            band: remaining * _BAND_WEIGHTS[band] // total_weight
            for band in active
        }
        progressed = False
        for band in sorted(active, key=lambda b: (-_BAND_WEIGHTS[b], -b)):
            give = min(shares[band], need[band] - allocation[band], remaining)
            if give > 0:
                allocation[band] += give
                remaining -= give
                progressed = True
        active = [band for band in active if allocation[band] < need[band]]
        if progressed:
            continue
        # 整数除法把份额抹成 0 时，按优先级逐个字节补足，避免小额预算被浪费。
        for band in active:
            if remaining <= 0:
                break
            allocation[band] += 1
            remaining -= 1
    return allocation


def bandwidth_budget(*args, **kwargs):
    """按优先级切分带宽预算，并报告关键状态是否被完整满足。"""

    state = _arguments(args, kwargs)
    budget = _as_int(state.get("budget", state.get("value", 0)), 0)

    needs = {band: 0 for band in PRIORITY_LABELS}
    raw_needs = state.get("needs", state.get("requests", {}))
    if isinstance(raw_needs, Mapping):
        for band, label in PRIORITY_LABELS.items():
            value = raw_needs.get(label, raw_needs.get(band, raw_needs.get(str(band), 0)))
            needs[band] = max(0, _as_int(value, 0))
    elif isinstance(raw_needs, (list, tuple)):
        for raw_packet in raw_needs:
            packet = _normalise_packet(raw_packet)
            needs[packet["priority"]] += packet["size"]

    allocation = _allocate(budget, needs)
    used = sum(allocation.values())

    return {
        "budget": budget,
        "used": used,
        "headroom": max(0, budget - used),
        "allocation": {label: allocation[band] for band, label in PRIORITY_LABELS.items()},
        "unmet": {
            label: max(0, needs[band] - allocation[band])
            for band, label in PRIORITY_LABELS.items()
        },
        "critical_guaranteed": allocation[3] == needs[3],
    }


def priority_quota(*args, **kwargs):
    """在预算内挑选增量包：critical 先选，其余按配额与稳定顺序取舍。"""

    state = _arguments(args, kwargs)
    budget = _as_int(state.get("budget", state.get("value", 0)), 0)

    raw_packets = state.get("packets", [])
    packets = [
        _normalise_packet(raw)
        for raw in (raw_packets if isinstance(raw_packets, (list, tuple)) else [])
    ]

    needs = {band: 0 for band in PRIORITY_LABELS}
    for packet in packets:
        needs[packet["priority"]] += packet["size"]
    allocation = _allocate(budget, needs)
    remaining = dict(allocation)

    ordered = sorted(
        packets,
        key=lambda packet: (-packet["priority"], packet["seq"], _canonical(packet["id"])),
    )

    selected = []
    dropped = []
    for packet in ordered:
        band = packet["priority"]
        if packet["size"] == 0 or remaining[band] >= packet["size"]:
            remaining[band] = max(0, remaining[band] - packet["size"])
            selected.append(packet)
        else:
            dropped.append({**packet, "reason": "quota"})

    # 整数取整留下的零头仍可被非关键包使用，但不会反向挤占 critical。
    used = sum(packet["size"] for packet in selected)
    if dropped and used < budget:
        leftover = budget - used
        still_dropped = []
        for packet in dropped:
            if packet["size"] <= leftover:
                selected.append(packet)
                leftover -= packet["size"]
                used += packet["size"]
            else:
                still_dropped.append(packet)
        dropped = still_dropped

    return {
        "budget": budget,
        "used": used,
        "attachment": {label: allocation[band] for band, label in PRIORITY_LABELS.items()},
        "selected": selected,
        "dropped": dropped,
        "critical_dropped": any(packet["priority"] == 3 for packet in dropped),
    }


# ---------------------------------------------------------------------------
# 3. 对象 ID 代际
# ---------------------------------------------------------------------------


def id_generation(*args, **kwargs):
    """为对象分配代际：删除后重建必须换新代，避免与旧对象撞 ID。"""

    state = _arguments(args, kwargs)
    entity_id = state.get("id", state.get("value"))
    op = str(state.get("op", OP_UPSERT))
    current = max(0, _as_int(state.get("current_generation", state.get("generation", 0)), 0))
    deleted = bool(state.get("deleted", state.get("tombstoned", False)))
    exists = bool(
        state.get(
            "exists",
            deleted
            or "current_generation" in state
            or "generation" in state
            or "entity" in state,
        )
    )

    if op == OP_DELETE:
        return {
            "id": entity_id,
            "generation": current,
            "action": "delete",
            "tombstone": True,
            "reused": False,
        }

    if deleted:
        return {
            "id": entity_id,
            "generation": current + 1,
            "action": "recreate",
            "tombstone": False,
            "reused": True,
        }

    if exists and state.get("force_new"):
        return {
            "id": entity_id,
            "generation": current + 1,
            "action": "recreate",
            "tombstone": False,
            "reused": True,
        }

    return {
        "id": entity_id,
        "generation": current,
        "action": "update" if exists else "create",
        "tombstone": False,
        "reused": False,
    }


def delete_new(*args, **kwargs):
    """处理“删除旧对象后立刻用同一 ID 新建”的场景。"""

    state = _arguments(args, kwargs)
    entity_id = state.get("id", state.get("value"))
    active_gen = _maybe_int(state.get("active_generation", state.get("current_generation")))
    deleted_gen = _maybe_int(state.get("tombstone_generation", state.get("deleted_generation")))
    deleted = bool(state.get("deleted", state.get("tombstoned", False)))

    if active_gen is not None and not deleted and not state.get("allow_reuse"):
        return {
            "id": entity_id,
            "generation": active_gen,
            "action": "conflict",
            "conflict": True,
            "reason": "id_still_active",
        }

    if deleted and active_gen is not None:
        deleted_gen = active_gen if deleted_gen is None else max(deleted_gen, active_gen)

    if deleted_gen is not None:
        return {
            "id": entity_id,
            "generation": deleted_gen + 1,
            "action": "create_after_delete",
            "conflict": False,
            "reused": True,
            "tombstone_generation": deleted_gen,
        }

    if active_gen is not None:
        return {
            "id": entity_id,
            "generation": active_gen + 1,
            "action": "recreate",
            "conflict": False,
            "reused": True,
            "tombstone_generation": active_gen,
        }

    return {
        "id": entity_id,
        "generation": 0,
        "action": "create",
        "conflict": False,
        "reused": False,
        "tombstone_generation": None,
    }


# ---------------------------------------------------------------------------
# 4. 序号幂等与乱序防护
# ---------------------------------------------------------------------------


def seq_idempotent(*args, **kwargs):
    """按流序号去重：旧序号拒绝、重复序号忽略、同序号不同内容交给合并规则。"""

    state = _arguments(args, kwargs)
    seq = _as_int(state.get("seq", state.get("value", 0)))
    last = _as_int(state.get("last_seq", -1))

    seen = {}
    raw_seen = state.get("seen")
    if isinstance(raw_seen, Mapping):
        for key, value in raw_seen.items():
            seen[_as_int(key, -1)] = value
    elif isinstance(raw_seen, (list, tuple, set, frozenset)):
        for item in raw_seen:
            candidate = _maybe_int(item)
            if candidate is not None:
                seen[candidate] = None

    payload = state.get("payload", state.get("attrs", state.get("op")))
    digest = _digest(payload) if payload is not None else None

    if seq < last:
        return {"apply": False, "reason": "stale", "last_seq": last, "digest": digest}
    if seq > last:
        if seq in seen and digest is not None and seen[seq] is not None and seen[seq] != digest:
            return {"apply": False, "reason": "conflict", "last_seq": last, "digest": digest}
        return {"apply": True, "reason": "new", "last_seq": seq, "digest": digest}

    # seq == last：重复包，不重复应用；内容不同则标记冲突交给 merge_rule。
    if digest is not None and seen.get(seq) is not None and seen[seq] != digest:
        return {"apply": False, "reason": "conflict", "last_seq": last, "digest": digest}
    return {"apply": False, "reason": "duplicate", "last_seq": last, "digest": digest}


def reorder_guard(*args, **kwargs):
    """流级乱序防护：连续序号才放行，缺口先缓冲，旧包直接丢弃。"""

    state = _arguments(args, kwargs)
    decision = _reorder(state)
    decision["buffered_count"] = len(decision["buffered"])
    return decision


# ---------------------------------------------------------------------------
# 5. 重连基线协商
# ---------------------------------------------------------------------------


def reconnect_base(*args, **kwargs):
    """重连时协商基线：版本一致才走增量，否则强制全量并重建基线。"""

    state = _arguments(args, kwargs)
    server_epoch = _as_int(state.get("server_epoch", state.get("epoch", 0)))
    client_epoch = _as_int(state.get("client_epoch", server_epoch))
    server_version = _as_int(
        state.get("server_version", state.get("version", state.get("value", 0)))
    )
    client_version = _as_int(state.get("client_version", server_version))
    window = max(0, _as_int(state.get("window", 64), 64))
    compacted_before = max(0, _as_int(state.get("compacted_before", 0), 0))

    def full(reason):
        return {
            "mode": "full",
            "full": True,
            "reason": reason,
            "base_epoch": server_epoch,
            "base_version": server_version,
            "from_version": server_version + 1,
            "target_version": server_version,
            "delta_versions": 0,
        }

    if client_epoch != server_epoch:
        return full("epoch_mismatch")
    if client_version > server_version:
        return full("client_ahead")
    if client_version < compacted_before:
        return full("compacted")
    if server_version - client_version > window:
        return full("outside_window")

    return {
        "mode": "delta",
        "full": False,
        "reason": "baseline_match",
        "base_epoch": server_epoch,
        "base_version": client_version,
        "from_version": client_version + 1,
        "target_version": server_version,
        "delta_versions": server_version - client_version,
    }


# ---------------------------------------------------------------------------
# 6. 冲突合并
# ---------------------------------------------------------------------------


def _merge_records(local, remote):
    if local["gen"] != remote["gen"]:
        if remote["gen"] > local["gen"]:
            return "remote", remote, "generation"
        return "local", local, "generation"

    # 同代际下墓碑优先：没有代际提升的迟到更新不能复活已删除对象。
    if local["deleted"] != remote["deleted"]:
        if local["deleted"]:
            return "local", local, "tombstone"
        return "remote", remote, "tombstone"

    if local["seq"] != remote["seq"]:
        if remote["seq"] > local["seq"]:
            return "remote", remote, "sequence"
        return "local", local, "sequence"

    if local["priority"] != remote["priority"]:
        if remote["priority"] > local["priority"]:
            return "remote", remote, "priority"
        return "local", local, "priority"

    # 序号与优先级都相同：用内容摘要做对称、可复现的裁决，
    # 保证两端各自合并也能得到同一结果。
    local_digest = _digest(local.get("attrs"))
    remote_digest = _digest(remote.get("attrs"))
    if local_digest != remote_digest:
        if remote_digest > local_digest:
            return "remote", remote, "stable_digest"
        return "local", local, "stable_digest"

    return "local", local, "identical"


def merge_rule(*args, **kwargs):
    """比较本地与远端记录，给出确定性的胜者。"""

    state = _arguments(args, kwargs)
    local_raw = state.get("local")
    remote_raw = state.get("remote", state.get("incoming"))

    if local_raw is None and remote_raw is None:
        return {"winner": None, "record": None, "reason": "no_input"}
    if remote_raw is None:
        return {"winner": "local", "record": _record(local_raw), "reason": "only_local"}
    if local_raw is None:
        return {"winner": "remote", "record": _record(remote_raw), "reason": "only_remote"}

    local = _record(local_raw)
    remote = _record(remote_raw)
    side, record, reason = _merge_records(local, remote)
    return {
        "winner": side,
        "record": record,
        "reason": reason,
        "local": local,
        "remote": remote,
    }


def _apply_ops(entities, tombstones, ops, counters):
    """把代际、墓碑与序号规则串起来，返回应用/跳过明细。"""

    applied = []
    skipped = []

    for index, raw in enumerate(ops):
        if not isinstance(raw, Mapping) or raw.get("id") is None:
            counters["invalid"] += 1
            skipped.append({"index": index, "reason": "invalid"})
            continue

        op = _record(raw)
        key = str(op["id"])
        gen = op["gen"]
        seq = op["seq"]
        tombstone_gen = tombstones.get(key, -1)
        current = entities.get(key)

        if op["deleted"]:
            if current is not None and current["gen"] > gen:
                counters["stale"] += 1
                skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": "stale_generation"})
                continue
            if tombstone_gen > gen:
                counters["stale"] += 1
                skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": "stale_tombstone"})
                continue

            tombstones[key] = max(tombstone_gen, gen)
            if current is not None and current["gen"] == gen:
                current["deleted"] = True
                current["op"] = OP_DELETE
                current["seq"] = max(current["seq"], seq)
            else:
                entities[key] = {**op, "deleted": True, "op": OP_DELETE}
            counters["tombstone"] += 1
            applied.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "op": OP_DELETE})
            continue

        if tombstone_gen >= gen:
            counters["stale"] += 1
            skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": "tombstoned"})
            continue
        if current is not None:
            if current["gen"] > gen:
                counters["stale"] += 1
                skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": "stale_generation"})
                continue
            if current["gen"] == gen:
                if current["deleted"]:
                    counters["conflict"] += 1
                    skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": "tombstone"})
                    continue
                if current["seq"] >= seq:
                    if current["seq"] == seq:
                        counters["duplicate"] += 1
                        reason = "duplicate"
                    else:
                        counters["stale"] += 1
                        reason = "stale_seq"
                    skipped.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "reason": reason})
                    continue

        entities[key] = {**op, "deleted": False, "op": OP_UPSERT}
        counters["applied"] += 1
        applied.append({"index": index, "id": op["id"], "gen": gen, "seq": seq, "op": OP_UPSERT})

    return applied, skipped


def delta_apply(*args, **kwargs):
    """应用一个增量包：先做流序号防护，再逐对象执行代际/墓碑/序号规则。"""

    state = _arguments(args, kwargs)
    raw_entities = state.get("entities")
    if not isinstance(raw_entities, Mapping):
        raw_entities = {}
    entities = {str(key): _record(value) for key, value in raw_entities.items()}

    raw_tombstones = state.get("tombstones")
    if not isinstance(raw_tombstones, Mapping):
        raw_tombstones = {}
    tombstones = {
        str(key): max(0, _as_int(value, 0)) for key, value in raw_tombstones.items()
    }
    counters = _zero_counters()

    packet = state.get("packet") if isinstance(state.get("packet"), Mapping) else {}
    raw_ops = state.get("ops")
    if raw_ops is None:
        raw_ops = packet.get("ops")
    if raw_ops is None:
        raw_ops = []
    if isinstance(raw_ops, Mapping):
        raw_ops = [raw_ops]
    if not isinstance(raw_ops, (list, tuple)):
        raw_ops = []

    packet_seq = packet.get("seq", state.get("packet_seq"))
    last_applied = _as_int(
        state.get("last_applied", state.get("last_applied_seq", packet.get("last_applied", 0)))
    )

    if packet_seq is not None:
        decision = _reorder(
            {
                "seq": _as_int(packet_seq),
                "last_applied": last_applied,
                "buffered": state.get("buffered"),
            }
        )
        if not decision["accept"]:
            counters["duplicate" if decision["reason"] == "duplicate" else "stale"] += 1
            return {
                "applied": [],
                "skipped": [{"seq": _as_int(packet_seq), "reason": decision["reason"]}],
                "entities": entities,
                "tombstones": tombstones,
                "metrics": counters,
                "last_applied": decision["last_applied"],
                "buffered": decision["buffered"],
                "rejected": True,
                "reason": decision["reason"],
            }
        last_applied = decision["last_applied"]

    applied, skipped = _apply_ops(entities, tombstones, raw_ops, counters)
    if packet_seq is not None:
        last_applied = max(last_applied, _as_int(packet_seq))

    version = _as_int(
        state.get("version", packet.get("version", state.get("base_version", 0))), 0
    )

    return {
        "applied": applied,
        "skipped": skipped,
        "entities": entities,
        "tombstones": tombstones,
        "metrics": counters,
        "last_applied": last_applied,
        "buffered": [],
        "version": version,
        "rejected": False,
        "reason": "applied" if applied else "noop",
    }


# ---------------------------------------------------------------------------
# 辅助入口
# ---------------------------------------------------------------------------


def validation(*args, **kwargs):
    """校验增量包结构与版本关系，返回结构化错误列表。"""

    state = _arguments(args, kwargs)
    packet = state.get("packet") if isinstance(state.get("packet"), Mapping) else None
    if packet is None and any(
        key in state for key in ("ops", "version", "seq", "base_version", "epoch")
    ):
        packet = state
    if packet is None:
        return {"valid": True, "errors": [], "checked": 0}
    if not isinstance(packet, Mapping):
        return {
            "valid": False,
            "errors": [{"code": "packet_not_object", "field": "packet", "message": "packet must be a mapping"}],
            "checked": 0,
        }

    errors = []

    def error(code, field, message):
        errors.append({"code": code, "field": field, "message": message})

    for field in ("epoch", "version", "seq", "base_version"):
        if field not in packet or packet[field] is None:
            continue
        value = _maybe_int(packet[field])
        if value is None:
            error("not_integer", field, f"{field} must be an integer")
        elif value < 0:
            error("negative", field, f"{field} must be >= 0")

    version = _maybe_int(packet.get("version"))
    base_version = _maybe_int(packet.get("base_version"))
    if version is not None and base_version is not None and base_version > version:
        error("base_ahead", "base_version", "base_version must not exceed version")

    ops = packet.get("ops", [])
    if ops is None:
        ops = []
    if not isinstance(ops, (list, tuple)):
        error("ops_not_list", "ops", "ops must be a list")
        ops = []

    seen = set()
    for index, raw in enumerate(ops):
        field = f"ops[{index}]"
        if not isinstance(raw, Mapping):
            error("op_not_object", field, "operation must be a mapping")
            continue
        if raw.get("id") is None:
            error("missing_id", f"{field}.id", "operation requires an entity id")
        for name in ("gen", "seq"):
            if name not in raw or raw[name] is None:
                continue
            value = _maybe_int(raw[name])
            if value is None:
                error("not_integer", f"{field}.{name}", f"{name} must be an integer")
            elif value < 0:
                error("negative", f"{field}.{name}", f"{name} must be >= 0")
        op_name = raw.get("op")
        if op_name is not None and op_name not in (OP_UPSERT, OP_DELETE):
            error("bad_op", f"{field}.op", "op must be 'upsert' or 'delete'")
        key = (str(raw.get("id")), _as_int(raw.get("gen", 0)), _as_int(raw.get("seq", 0)))
        if key in seen:
            error("duplicate_op", field, "duplicate (id, gen, seq) in one packet")
        seen.add(key)

    return {"valid": not errors, "errors": errors, "checked": len(ops)}


def audit(*args, **kwargs):
    """把决策事件整理成稳定、可回放的审计记录。"""

    state = _arguments(args, kwargs)
    raw_events = state.get("events", [])
    if not isinstance(raw_events, (list, tuple)):
        raw_events = []

    records = []
    for index, event in enumerate(raw_events):
        if not isinstance(event, Mapping):
            continue
        records.append(
            {
                "index": index,
                "kind": str(event.get("kind", "event")),
                "id": event.get("id"),
                "gen": _as_int(event.get("gen", 0)),
                "seq": _as_int(event.get("seq", 0)),
                "priority": _priority(event.get("priority", 1)),
                "reason": event.get("reason"),
                "detail": event.get("detail"),
            }
        )

    records.sort(
        key=lambda record: (
            record["gen"],
            record["seq"],
            _canonical(record["id"]),
            record["kind"],
            str(record["reason"]),
        )
    )
    return records


def metrics(*args, **kwargs):
    """汇总同步指标；可由计数器映射或事件流直接计算。"""

    state = _arguments(args, kwargs)
    counters = _zero_counters()

    raw_counters = state.get("counters")
    if isinstance(raw_counters, Mapping):
        for key in _COUNTER_KEYS:
            counters[key] = max(0, _as_int(raw_counters.get(key, 0), 0))
        for key, value in raw_counters.items():
            if key not in counters:
                counters[str(key)] = max(0, _as_int(value, 0))

    raw_events = state.get("events")
    if isinstance(raw_events, (list, tuple)):
        for event in raw_events:
            if not isinstance(event, Mapping):
                continue
            kind = str(event.get("kind", ""))
            if kind in counters:
                counters[kind] += 1

    total = sum(counters.values())
    return {**counters, "total": total}


def determinism(*args, **kwargs):
    """对增量做规范化排序与去重，输出稳定摘要供两端比对。"""

    state = _arguments(args, kwargs)
    raw_ops = state.get("ops", [])
    if isinstance(raw_ops, Mapping):
        raw_ops = [raw_ops]
    if not isinstance(raw_ops, (list, tuple)):
        raw_ops = []

    canonical = []
    seen = set()
    for raw in raw_ops:
        record = _record(raw)
        encoded = _canonical(record)
        if encoded in seen:
            continue
        seen.add(encoded)
        canonical.append((record, encoded))

    canonical.sort(
        key=lambda pair: (
            pair[0]["gen"],
            pair[0]["seq"],
            _canonical(pair[0]["id"]),
            pair[0]["op"],
            pair[1],
        )
    )

    digest = hashlib.sha256("\n".join(encoded for _, encoded in canonical).encode("utf-8")).hexdigest()
    return {
        "count": len(canonical),
        "digest": digest,
        "ops": [record for record, _ in canonical],
    }
