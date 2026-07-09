from stockodile.util.time import ms_to_ns, now_ns, rfc3339_to_ns, us_to_ns


def test_ms_to_ns() -> None:
    assert ms_to_ns(1) == 1_000_000
    assert ms_to_ns(2.5) == 2_500_000


def test_us_to_ns() -> None:
    assert us_to_ns(1) == 1_000
    assert us_to_ns(2.5) == 2_500


def test_now_ns() -> None:
    ns = now_ns()
    assert isinstance(ns, int)
    assert ns > 0


def test_rfc3339_to_ns_utc_z() -> None:
    # 2026-07-09T04:00:00Z -> epoch seconds = 1783569600
    expected = 1783569600 * 1_000_000_000
    assert rfc3339_to_ns("2026-07-09T04:00:00Z") == expected


def test_rfc3339_to_ns_offset_plus() -> None:
    # 2026-07-09T07:00:00+03:00 -> should be 2026-07-09T04:00:00 UTC
    expected = 1783569600 * 1_000_000_000
    assert rfc3339_to_ns("2026-07-09T07:00:00+03:00") == expected


def test_rfc3339_to_ns_offset_minus() -> None:
    # 2026-07-08T23:00:00-05:00 -> should be 2026-07-09T04:00:00 UTC
    expected = 1783569600 * 1_000_000_000
    assert rfc3339_to_ns("2026-07-08T23:00:00-05:00") == expected


def test_rfc3339_to_ns_with_subseconds() -> None:
    # 2026-07-09T04:00:00.123456Z
    expected = 1783569600 * 1_000_000_000 + 123456000
    assert rfc3339_to_ns("2026-07-09T04:00:00.123456Z") == expected


def test_rfc3339_to_ns_with_subseconds_and_offset() -> None:
    # 2026-07-09T07:00:00.123456+03:00
    expected = 1783569600 * 1_000_000_000 + 123456000
    assert rfc3339_to_ns("2026-07-09T07:00:00.123456+03:00") == expected


def test_rfc3339_to_ns_no_offset_no_z() -> None:
    # default offset is +00:00
    expected = 1783569600 * 1_000_000_000
    assert rfc3339_to_ns("2026-07-09T04:00:00") == expected


def test_rfc3339_to_ns_subseconds_no_offset() -> None:
    # default offset is +00:00
    expected = 1783569600 * 1_000_000_000 + 123456000
    assert rfc3339_to_ns("2026-07-09T04:00:00.123456") == expected
