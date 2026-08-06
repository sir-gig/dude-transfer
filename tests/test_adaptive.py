from dude_transfer.adaptive import AdaptiveConcurrency


def test_adaptive_controller_probes_and_backs_off():
    controller = AdaptiveConcurrency(initial=2, maximum=8, window_parts=2, started_at=0)

    assert controller.observe(byte_count=10 * 1024**2, completed_at=0.5, attempts=1) is None
    increase = controller.observe(byte_count=10 * 1024**2, completed_at=1.0, attempts=1)
    assert increase.current == 3
    assert increase.reason == "throughput_probe"

    decrease = controller.observe(byte_count=10 * 1024**2, completed_at=2.0, attempts=2)
    assert decrease.current == 1
    assert decrease.reason == "retry_backoff"


def test_adaptive_controller_never_exceeds_ceiling():
    controller = AdaptiveConcurrency(initial=1, maximum=2, window_parts=1, started_at=0)
    first = controller.observe(byte_count=10 * 1024**2, completed_at=1, attempts=1)
    assert first.current == 2
    assert controller.observe(byte_count=20 * 1024**2, completed_at=2, attempts=1) is None
    assert controller.current == 2
    assert controller.peak == 2
