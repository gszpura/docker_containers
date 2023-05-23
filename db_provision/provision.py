import os
import enum
import uuid
import argparse
import asyncio
import itertools
import asyncpg
from pydantic import BaseSettings
import random
import string
import datetime


class Config(BaseSettings):
    DATABASE_USER: str = "app_user"
    DATABASE_PASSWORD: str = "app_password"
    DATABASE_HOST: str = "172.20.0.10"
    DATABASE_NAME: str = "sensor"
    DATABASE_PORT: int = 5432
    DATABASE_POOL_SIZE: int = 5
    DATABASE_MAX_OVERFLOW: int = 50


class KeyRegister:

    def __init__(self):
        self.foreign_keys = {}

    def get_keys(self, entity: str):
        return self.foreign_keys.get(entity, [])

    def set_keys(self, entity: str, values: list):
        self.foreign_keys[entity] = values


class PoolManager:

    def __init__(self):
        self.pool = None

    async def init(self):
        self.pool = await self._create_pool()

    async def _create_pool(self):
        pool = await asyncpg.create_pool(
            user=settings.DATABASE_USER,
            password=settings.DATABASE_PASSWORD,
            database=settings.DATABASE_NAME,
            host=settings.DATABASE_HOST,
            min_size=settings.DATABASE_POOL_SIZE,
            max_size=settings.DATABASE_MAX_OVERFLOW,
        )
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return pool

    def get_pool(self):
        if self.pool is None:
            raise Exception("Pool not initialised...")
        return self.pool


# singletons

settings = Config()
POOL = PoolManager()
KEY_REGISTER = KeyRegister()


def get_sql_files(path):
    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(".sql"):
                files.append(os.path.join(dirpath, filename))
    return files


def get_queries(filename: str):
    print(f"Checking {filename}")
    with open(filename, 'r') as rd:
        content = rd.read()
        qs = [q for q in content.split("\n\n") if q]
        return qs


async def run_execute(queries: list[str]):
    pool = POOL.get_pool()
    resp_list = []
    async with pool.acquire() as conn:
        for q in queries:
            try:
                await conn.execute(q)
            except asyncpg.exceptions.PostgresSyntaxError as exc:
                print("Issue while executing a query:", q, exc)
            except asyncpg.exceptions.UndefinedTableError:
                print("Undefined table... Skipping.")
            except asyncpg.exceptions.IntegrityConstraintViolationError as exc:
                print("Inconsistent data:", q, exc)
            except asyncpg.exceptions.SyntaxOrAccessError as exc:
                print("Issue while running a query:", exc)
    return resp_list


async def run_fetch(queries: list[str]):
    pool = POOL.get_pool()
    resp_list = []
    async with pool.acquire() as conn:
        for q in queries:
            try:
                resp = await conn.fetch(q)
                resp_list.append(resp)
            except asyncpg.exceptions.PostgresSyntaxError as exc:
                print("Issue while executing a query:", q, exc)
            except asyncpg.exceptions.UndefinedTableError:
                print("Undefined table... Skipping.")
            except asyncpg.exceptions.IntegrityConstraintViolationError as exc:
                print("Inconsistent data:", q, exc)
            except asyncpg.exceptions.SyntaxOrAccessError as exc:
                print("Issue while running a query:", exc)
    return resp_list


async def provision(filename: str = None):
    if not filename:
        files = get_sql_files('.')
    else:
        files = [filename]
    q = [get_queries(f) for f in files]
    qs = list(itertools.chain(*q))
    await run_execute(qs)


class DataTypeEnum(enum.Enum):
    INT = "int"
    UUID = "uuid"
    NUMERIC = "numeric"
    VARCHAR = "varchar"
    FOREIGN_KEY = "fkey"
    TIMESTAMP = "timestamp"


class DataType:

    def __init__(self, name: str, type: DataTypeEnum, parameter: int | str):
        self.type = type
        self.name = name
        self.parameter = parameter


async def get_foreign_key_values(table_name):
    query = f"SELECT id FROM {table_name} LIMIT 1000;"
    keys = KEY_REGISTER.get_keys(table_name)
    if not keys:
        results: list[list[asyncpg.Record]] = await run_fetch([query])
        KEY_REGISTER.set_keys(table_name, [v['id'] for v in results[0]])
        keys = KEY_REGISTER.get_keys(table_name)
    return keys


async def generate_single_row(schema: list[DataType]):
    values = []
    for dt in schema:
        type, param = dt.type, dt.parameter
        if type == DataTypeEnum.INT:
            values.append(random.randint(0, 1000))
        if type == DataTypeEnum.UUID:
            values.append(uuid.uuid4())
        if type == DataTypeEnum.NUMERIC:
            values.append(random.random() * 10)
        if type == DataTypeEnum.VARCHAR:
            values.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=int(param))))
        if type == DataTypeEnum.TIMESTAMP:
            values.append(datetime.datetime.now())
        if type == DataTypeEnum.FOREIGN_KEY:
            keys = await get_foreign_key_values(param)
            total_len = len(keys)
            values.append(keys[random.randint(0, total_len - 1)])
    return values


async def generate_data_from_types(schema: list[DataType], size=10):
    values = []
    for i in range(size):
        data = await generate_single_row(schema)
        values.append(data)
    return values


async def create_insert_queries(table_name: str, schema: list[DataType], values: list) -> str:
    """
    TODO: make it readable for normal human beings
    :param table_name:
    :param schema:
    :param values:
    :return:
    """
    parts = [f"INSERT INTO {table_name}("]
    for column in schema:
        parts.append(f'{column.name}, ')
    parts[-1] = parts[-1].strip(", ")
    parts.append(") VALUES (")
    for item in values:
        for i, column_val in enumerate(item):
            col_type = schema[i].type
            if col_type == DataTypeEnum.INT:
                parts.append(f"{column_val}, ")
            else:
                parts.append(f"'{column_val}', ")
        parts[-1] = parts[-1].strip(", ")
        parts.append("), (")
    parts[-1] = parts[-1].strip(", (")
    parts.append(";")
    return "".join(parts)


def get_table_schema(name):
    """
    TODO: create it form .sql files,
    update parameters
    :param name:
    :return:
    """
    if name == "location":
        return [
            DataType("id", DataTypeEnum.INT, "not important"),
            DataType("created_at", DataTypeEnum.TIMESTAMP, "not important"),
            DataType("address", DataTypeEnum.VARCHAR, 10)
        ]
    elif name == "sensor":
        return [
            DataType("id", DataTypeEnum.UUID, "not important"),
            DataType("location_id", DataTypeEnum.FOREIGN_KEY, "location"),
            DataType("type", DataTypeEnum.VARCHAR, 10)
        ]
    elif name == "measurement":
        return [
            DataType("id", DataTypeEnum.UUID, "not important"),
            DataType("created_at", DataTypeEnum.TIMESTAMP, "not important"),
            DataType("sensor_id", DataTypeEnum.FOREIGN_KEY, "sensor"),
            DataType("value", DataTypeEnum.NUMERIC, "not important")
        ]
    else:
        return []


async def insert_generate(order: list[str]):
    """
    TODO: order should be figured out by topological sort
    :param order:
    :return:
    """
    for entity in order:
        print("Inserting for:", entity)
        data = await generate_data_from_types(get_table_schema(entity))
        query = await create_insert_queries(entity, get_table_schema(entity), data)
        await run_execute([query])


async def main():
    parser = argparse.ArgumentParser(
        prog='Provision',
        description='DB',
        epilog='DB Helper'
    )
    parser.add_argument('-i', '--insert', action='store_true')
    parser.add_argument('-f', '--file')

    args = parser.parse_args()
    await POOL.init()
    if args.insert:
        await insert_generate(["location", "sensor", "measurement"])
    else:
        if args.file:
            await provision(args.file)
        else:
            await provision()


if __name__ == "__main__":
    asyncio.run(main())
