# tests/providers/test_sec_rate_gate_holder.py
from app.providers import sec_rate_gate_holder as holder
from app.providers.rate_gate import InProcessFloorGate


def test_default_is_inprocess_floor():
    holder._reset_sec_rate_gate_for_tests()
    assert isinstance(holder.get_sec_rate_gate(), InProcessFloorGate)


def test_set_then_get_returns_set_gate():
    holder._reset_sec_rate_gate_for_tests()
    sentinel = InProcessFloorGate(floor=0.5)
    holder.set_sec_rate_gate(sentinel)
    assert holder.get_sec_rate_gate() is sentinel


def test_getter_reflects_swap_for_late_importer():
    # Simulates §3b: a module that imports the holder (not the gate value)
    # still sees a gate set AFTER it imported.
    holder._reset_sec_rate_gate_for_tests()
    from app.providers import sec_rate_gate_holder as late_import

    swapped = InProcessFloorGate(floor=0.9)
    holder.set_sec_rate_gate(swapped)
    assert late_import.get_sec_rate_gate() is swapped
