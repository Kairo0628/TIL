**fixture01.py**
- 단순한 fixture 사용

**fixture02.py**
- fixture에서 fixture 사용
- 하나의 fixture에서 두 개의 fixture 사용

**fixture03.py**
- fixture는 매 테스트마다 한 번씩 실행

**fixture04.py**
- 하나의 테스트에서 fixture를 여러번 요청하기
- `test_request_twice`, `append_first`에서 `empty_list`를 두 번 요청

**scope/**
- `fixture(scope = 'some_val')`에 대한 실험
- `scope/` 디렉터리에서 `pytest -v -s test_scope1.py test_scope2.py pkg/test_scope3.py`로 실행
