"""Tests for rustvello.get_version() and package import."""

import rustvello


def test_get_version_returns_string():
    version = rustvello.get_version()
    assert isinstance(version, str)
    assert len(version) > 0


def test_version_attribute_matches():
    assert rustvello.__version__ == rustvello.get_version()
