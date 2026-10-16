import pytest

@pytest.fixture
def value():
    return []

def test_append_one(value):
    value.append(1)
    assert value == [1]

def test_append_two(value):
    value.append(2)
    assert value == [1, 2]
