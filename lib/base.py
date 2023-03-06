import functools


def pipe(*functions):
    """
    Glue together functions operating on spark DataFrames in a single pipeline.
    Example:
    If we have two functions a(df) and b(df) returning a new dataframe with some
    change we can glue them together (a -> b) (df) which will first invoke
    function a, and then pass the result to function b.
    Note that this compose is not a classical math compose which would first
    call b, and then pass the result to a.

    Parameters:
        ...functions (DataFrameFn) - variable parameters of DF functions

    Returns:
        (DataFrame > DataFrame) - final composed (pipeline) function
    """
    return functools.reduce(
        lambda f, g: lambda x: g(f(x)), functions, lambda x: x
    )


def filter(filter_expression):
    """
    Create a function to apply the filter expression to dataframe

    Parameters:
        filter_expression (str)

    Returns:
        (DataFrame > DataFrame) - requested function
    """

    def apply(df):
        return df.filter(filter_expression)

    return apply


def join(table, on, how="inner", columns=["*"]):
    """
    Helper function to join the dataframes and select
    """

    if isinstance(columns, str):
        columns = [columns]

    def apply(source):
        return source.join(table.select(*columns), on, how)

    return apply


def log(msg):
    """
    Helper function to print the dataframe in the middle of pipeline
    """

    def apply(df):
        print(msg)
        df.show()
        return df

    return apply


def select(*columns):
    def apply(df):
        return df.select(*columns)

    return apply
