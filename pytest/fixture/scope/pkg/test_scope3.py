import pytest

def test_func_3(fixture_pkg, fixture_sess):
    fixture_pkg.append(1)
    fixture_sess.append(1)

    assert fixture_pkg == [1, 1, 1, 1, 1]
    assert fixture_sess == [1, 1, 1, 1, 1]
