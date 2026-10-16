import pytest

@pytest.fixture
def empty_list():
    return []

@pytest.fixture
def append_first(empty_list):
    return empty_list.append(1)

def test_request_twice(append_first, empty_list):
    assert empty_list == [1]
