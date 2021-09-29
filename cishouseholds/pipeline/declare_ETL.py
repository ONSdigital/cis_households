ETL_scripts = {}

# this is a parametered decorator
def add_ETL(key):  # noqa E731
    # this is the actual decorator
    def _add_ETL(func):
        ETL_scripts[key] = func
        return func

    return _add_ETL
