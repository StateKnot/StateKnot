<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Run execution

StateKnot preserves the identity, ownership and accounting of work across Run
recovery. These terms distinguish durable facts from permission to execute.

## Language

**Join handoff**:
The durable suspension of a child-owning node while its exact set of child Runs
settles. The handoff preserves the original physical attempt without declaring
that attempt successful.
_Avoid_: Join completion, child success

**Physical attempt**:
One execution of a logical node activation under a particular Run fence.
Its recorded history remains distinct from any later execution of that activation.
_Avoid_: Logical activation, current execution permission

**Frame caller**:
The framework-owned node attempt whose declared call suspended a parent graph
until the called frame returns or closes.
_Avoid_: Child Run owner, ordinary node

**DIRECT usage**:
The complete usage attributable to a Run's own work, including its same-Run graph
frames, before adding the settled usage of isolated child Runs.
_Avoid_: Total usage, structural minimum

**Delegated usage**:
The settled cumulative usage of isolated child Run subtrees attributable to their
owning Run. Each subtree contributes once to that owner's total usage.
_Avoid_: Frame usage, reservation
