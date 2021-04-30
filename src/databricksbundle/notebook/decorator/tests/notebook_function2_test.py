from databricksbundle.notebook.decorator.notebook_function import notebook_function

try:

    @notebook_function
    def load_data():
        return 155

    raise Exception("Notebook function without parentheses must fail")

except Exception as e:
    assert str(e) == "Use @notebook_function() instead of @notebook_function please"
