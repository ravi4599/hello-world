create VIEW IF NOT EXISTS FLYWAY_TEST_BV (
    test_col,
    test_col_2 
) as
select test_col, test_col_2 
from RDS_%%envname%%.DUN.FLYWAY_TEST;