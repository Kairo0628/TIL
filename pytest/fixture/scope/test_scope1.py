import pytest

def test_func_1(
    fixture_func,
    fixture_mod,
    fixture_pkg,
    fixture_sess
):
    fixture_func.append(1)
    fixture_mod.append(1)
    fixture_pkg.append(1)
    fixture_sess.append(1)

    assert fixture_func == [1]
    assert fixture_mod == [1]
    assert fixture_pkg == [1]
    assert fixture_sess == [1]

class TestClass1():
    def test_cls1_func1(
        self,
        fixture_func,
        fixture_cls,
        fixture_mod,
        fixture_pkg,
        fixture_sess
    ):
        
        fixture_func.append(1)
        fixture_cls.append(1)
        fixture_mod.append(1)
        fixture_pkg.append(1)
        fixture_sess.append(1)

        assert fixture_func == [1]
        assert fixture_cls == [1]
        assert fixture_mod == [1, 1]
        assert fixture_pkg == [1, 1]
        assert fixture_sess == [1, 1]

    def test_cls1_func2(
        self,
        fixture_cls,
        fixture_mod,
        fixture_pkg,
        fixture_sess
    ):
        
        fixture_cls.append(1)
        fixture_mod.append(1)
        fixture_pkg.append(1)
        fixture_sess.append(1)

        assert fixture_cls == [1, 1]
        assert fixture_mod == [1, 1, 1]
        assert fixture_pkg == [1, 1, 1]
        assert fixture_sess == [1, 1, 1]

class TestClass2():
    def test_cls2_func1(
        self,
        fixture_mod,
        fixture_pkg,
        fixture_sess
    ):
        
        fixture_mod.append(1)
        fixture_pkg.append(1)
        fixture_sess.append(1)

        assert fixture_mod == [1, 1, 1, 1]
        assert fixture_pkg == [1, 1, 1, 1]
        assert fixture_sess == [1, 1, 1, 1]
