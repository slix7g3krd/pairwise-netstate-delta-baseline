import pytest
import gameplay.netstate_delta_baseline as mod

CASES = [
    ("baseline_ack", (1)),
    ("priority_quota", (1)),
    ("id_generation", (1)),
    ("seq_idempotent", (1)),
    ("reconnect_base", (1)),
    ("merge_rule", (1)),
    ("delta_apply", (1)),
    ("validation", (1)),
    ("audit", (1)),
    ("fallback", (1)),
    ("bandwidth_budget", (1)),
    ("metrics", (1)),
    ("delete_new", (1)),
    ("reorder_guard", (1)),
    ("determinism", (1)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
