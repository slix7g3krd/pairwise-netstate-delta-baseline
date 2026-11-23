# pairwise-netstate-delta-baseline

## 领域

游戏、图形与多媒体

## 主题

状态增量与基线

## 说明

基线重传、优先级配额与 ID 代际。

初始状态下 `gameplay/netstate_delta_baseline.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。
