def add(a, b):
    return a + b

def divide(a, b):
    if b == 0:
        raise ZeroDivisionError()
    
    return a / b

def classify_num(a):
    if a > 0:
        return 'pos'
    elif a < 0:
        return 'neg'
    else:
        return 'zero'
