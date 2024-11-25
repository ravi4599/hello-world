#!/usr/bin/env python
from os import getenv, path, makedirs, getcwd, chdir
from re import sub
from snowflake.connector import connect
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()

    parser.add_argument(
        '--snowflake-database',
        default=getenv('SNOWFLAKE_DATABASE'),
        type=str,
        help='Snowflake Database'
    )

    parser.add_argument(
        '--snowflake-user',
        default=getenv('SNOWFLAKE_USER'),
        type=str,
        help='Snowflake User'
    )

    parser.add_argument(
        '--snowflake-password',
        default=getenv('SNOWFLAKE_PASSWORD'),
        type=str,
        help='Snowflake Password'
    )

    parser.add_argument(
        '--snowflake-account',
        default=getenv('SNOWFLAKE_ACCOUNT', 'inspire'),
        type=str,
        help='Snowflake Account'
    )

    parser.add_argument(
        '--snowflake-region',
        default=getenv('SNOWFLAKE_REGION', 'east-us-2.azure'),
        type=str,
        help='Snowflake Region'
    )

    parser.add_argument(
        '--snowflake-role',
        default=getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        type=str,
        help='Snowflake Role'
    )

    parser.add_argument(
        '--file-per-schema',
        default="True",
        type=str,
        help='Create separate file per schema or not'
    )


    parser.add_argument(
        '--baseline',
        default="True",
        type=str,
        help='Is it a baseline?'
    )

    parsed_args = parser.parse_args()

    return parsed_args


def get_ddl(type, name, baseline):
    print(f"Creating DDL for {name}")

    if baseline:
        output_file = f"./B1__baseline_{name}.sql"
    else:
        output_file= f"./V1___{name}.sql"

    cs = ctx.cursor()
    try:
        cs.execute(f"SELECT GET_DDL('{type}', '{name}', TRUE);")
        results = cs.fetchall()
        # NOTE: formate as string and removes two lines with database creation statement
        result = "".join(results[0]).split("\n", 2)
        if len(result) < 3:
            result = ""
        else:
            result = result[2]
        # NOTE: cleanup names
        result = sub(rf"(?i){name}\.", "", result)
        result = sub(r"(?i)or replace (database|schema|view|table|procedure|function|file format|sequence)", r"\1 IF NOT EXISTS", result)


        with open(output_file, "w") as baseline:
            baseline.write("".join(result))
    finally:
        cs.close()

cmd_args = parse_args()

database = cmd_args.snowflake_database
baseline = cmd_args.baseline

ctx = connect(
    user=cmd_args.snowflake_user,
    password=cmd_args.snowflake_password,
    account=f"{cmd_args.snowflake_account}.{cmd_args.snowflake_region}",
    database=database,
    role=cmd_args.snowflake_role,
    )

if cmd_args.file_per_schema == "True":
    print(f"Creating DDL per schema")
    cs = ctx.cursor()
    try:
        cs.execute(f"SHOW SCHEMAS in {database};")
        schemas = cs.fetchall()
    finally:
        cs.close()
    for schema in schemas:
        name = schema[1]
        if name in ['PUBLIC', 'INFORMATION_SCHEMA']:
            print(f"Skipping schema {name}")
            continue
        full_name = f'{database}.{name}'
        current_dir = getcwd()
        schmema_dir = f'{current_dir}/{name}'

        if not path.exists(schmema_dir):
            makedirs(schmema_dir)

        chdir(schmema_dir)
        get_ddl('SCHEMA', full_name, baseline)
        chdir(current_dir)
else:
    print("Creating DDL for entire DB")
    get_ddl('DATABASE', database)
ctx.close()
