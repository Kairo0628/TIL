import pytest

@pytest.fixture
def value():
    return [1, 2, 3]

def test_value(value):
    assert sum(value) == 6

def test_value_fail(value):
    assert sum(value) == 5
