# Test file for pre-commit Python formatting


def test_function(param1, param2):
    # This function has intentional formatting issues
    result = param1 + param2
    print("Testing pre-commit hooks")
    return result


class TestClass:
    def __init__(self):
        pass

    def method_with_issues(self):
        x = 1
        y = 2
        z = x + y
        return z
