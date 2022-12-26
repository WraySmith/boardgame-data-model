from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def quality_check(spark, data, columns, count, table_name):
    """
    Provides quality checks on a spark DataFrame. Checks
    that there are rows in DataFrame, there are correct
    number of columns, and that any columns that should not
    have any null values do not have null values/

    Args:
        spark: spark session
        data: spark DataFrame
        columns: columns to be checked for null values
            (list(str)
        count: expected number of columns in the DataFrame
            (int)
        table_name: name of Table the DataFrame corresponds with

    Returns:
        True if all checks pass; otherwise False
    """

    total_checks = 2 + len(columns)
    fail_count = 0
    pass_count = 0
    col_count = len(data.columns)
    row_count = data.count()

    if row_count == 0:
        print(f"Row check failed: {table_name} has no rows")
        fail_count += 1
    else:
        print(f"Row check passed: {table_name} has {row_count} rows")
        pass_count += 1

    if col_count != count:
        print(
            f"Column check failed: {table_name} has {col_count} instead of {count} columns"
        )
        fail_count += 1
    else:
        print(f"Column check passed: {table_name} has {col_count} columns")
        pass_count += 1

    for check in columns:
        check_null = data.filter(col(check).isNull()).count()
        if check_null > 0:
            print(
                f"Null check failed for {check} column in {table_name}, there are {check_null} values"
            )
            fail_count += 1
        else:
            print(f"Null check passed for {check} column in {table_name}")
            pass_count += 1

    print(f"Passed checks out of total = {pass_count} / {total_checks} in {table_name}")
    print(f"Failed checks out of total = {fail_count} / {total_checks} in {table_name}")
    
    return (fail_count == 0)