"""Group CASCADE_WAITING layers under their terminal-blocked root (spec §6).

Pure function over a dependency map + a state map. Used by the v2 API
endpoint to produce `cascade_groups`; replaces the inline shim in
`app/api/sync.py`.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.services.sync_orchestrator.layer_types import LayerState


@dataclass(frozen=True)
class ProblemGroup:
    root: str
    affected: list[str]


def collapse_cascades(
    dependencies: dict[str, tuple[str, ...]],
    states: dict[str, LayerState],
) -> list[ProblemGroup]:
    """Return one ProblemGroup per ACTION_NEEDED / SECRET_MISSING layer,
    each listing every CASCADE_WAITING descendant in the DAG.

    `dependencies[name]` lists the direct upstream layer names for
    `name`. `states[name]` is the layer's current LayerState. Layers
    missing from either map are treated as absent.
    """
    # Reverse adjacency: parent -> list of children. Computed once.
    reverse: dict[str, list[str]] = {n: [] for n in dependencies}
    for name, deps in dependencies.items():
        for dep in deps:
            reverse.setdefault(dep, []).append(name)

    terminal = {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
    roots = [n for n, s in states.items() if s in terminal]

    groups: list[ProblemGroup] = []
    for root in roots:
        affected: list[str] = []
        frontier = {root}
        visited = {root}
        while frontier:
            next_frontier: set[str] = set()
            for parent in frontier:
                for child in reverse.get(parent, ()):
                    if child in visited:
                        continue
                    visited.add(child)
                    if states.get(child) is LayerState.CASCADE_WAITING:
                        affected.append(child)
                    next_frontier.add(child)
            frontier = next_frontier
        groups.append(ProblemGroup(root=root, affected=affected))
    return groups
