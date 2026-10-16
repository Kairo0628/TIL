import pytest

@pytest.fixture
def fixture_func():
    yield []
    print('\n Function Fixture 사용 종료')

@pytest.fixture(scope = 'class')
def fixture_cls():
    yield []
    print('\n Class Fixture 사용 종료')

@pytest.fixture(scope = 'module')
def fixture_mod():
    yield []
    print('\n Module Fixture 사용 종료')

@pytest.fixture(scope = 'package')
def fixture_pkg():
    yield []
    print('\n Package Fixture 사용 종료')

@pytest.fixture(scope = 'session')
def fixture_sess():
    yield []
    print('\n Session Fixture 사용 종료')
