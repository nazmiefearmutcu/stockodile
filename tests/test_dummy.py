def test_dummy():
    print("HELLO FROM DUMMY TEST")
    assert True


def test_package_version_matches_pyproject() -> None:
    import re
    from pathlib import Path

    import stockodile

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    m = re.search(r'^version\s*=\s*"([^"]+)"', pyproject.read_text(), re.M)
    assert m is not None
    # When not installed, __init__ falls back to same release string
    assert stockodile.__version__ in (m.group(1), f"{m.group(1)}")
