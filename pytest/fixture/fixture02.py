import pytest

@pytest.fixture
def val1():
    return [1, 2]

@pytest.fixture
def val2(val1):
    return val1 + [3, 4]

def test_fixture_other_fixture(val2):
    assert val2 == [1, 2, 3, 4]

@pytest.fixture
def val3():
    return [4, 5]

def test_requests_two_fixtures(val1, val3):
    assert val1 + val3 == [1, 2, 4, 5]
