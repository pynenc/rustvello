"""Tests for TaskId and InvocationId."""

import pytest

from rustvello import InvocationId, TaskId


class TestTaskId:
    def test_create_valid(self):
        t = TaskId("my_module", "my_func")
        assert t.language == "python"
        assert t.module == "my_module"
        assert t.name == "my_func"

    def test_str_contains_both(self):
        t = TaskId("mod", "fn")
        s = str(t)
        assert s.startswith("python::")
        assert "mod" in s
        assert "fn" in s

    def test_repr_format(self):
        t = TaskId("mod", "fn")
        r = repr(t)
        assert r.startswith("TaskId(")


class TestInvocationId:
    def test_new_generates_unique(self):
        a = InvocationId()
        b = InvocationId()
        assert str(a) != str(b)

    def test_from_string_valid_uuid(self):
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        inv_id = InvocationId.from_string(uuid_str)
        assert str(inv_id) == uuid_str

    def test_from_string_invalid_raises(self):
        with pytest.raises(ValueError):
            InvocationId.from_string("not-a-uuid")

    def test_repr_format(self):
        inv_id = InvocationId()
        r = repr(inv_id)
        assert r.startswith("InvocationId::from('")
