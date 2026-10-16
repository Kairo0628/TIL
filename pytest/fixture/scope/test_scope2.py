import pytest

def test_func_2(fixture_mod):
    fixture_mod.append(1)

    assert fixture_mod == [1]
