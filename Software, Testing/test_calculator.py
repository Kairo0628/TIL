import pytest
from calculator import add, divide, classify_num

def test_add():
    assert add(1, 2) == 3

def test_divide():
    assert divide(4, 2) == 2

def test_zero_division_error():
    with pytest.raises(ZeroDivisionError):
        divide(4, 0)

def test_classify_num():
    assert classify_num(1) == 'pos'
    assert classify_num(-1) == 'neg'
