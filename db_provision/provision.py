import uuid
import argparse
import asyncio
import asyncpg
import random
import string
import datetime
from config import settings
from sql import DataType, DataTypeEnum, SQLParser, Table, JobTable, KeyTypeEnum, CommandTypeEnum
from query_finder import QueryFinder


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
POOL = PoolManager()  # TODO: to be part of Generator class and Provision class
KEY_REGISTER = KeyRegister()  # TODO: to be part of Generator class


async def run_execute(queries: list[str]):
    """
    TODO: part of Pool/DB class
    :param queries:
    :return:
    """
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
    """
    TODO: part of Pool/DB class
    :param queries:
    :return:
    """
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


async def get_foreign_key_values(field: DataType):
    """
    TODO: Move to generator class
    :param field:
    :return:
    """
    keys = KEY_REGISTER.get_keys(field.get_ref_table())
    if not keys:
        results: list[list[asyncpg.Record]] = await run_fetch([field.get_select_for_foreign_key()])
        KEY_REGISTER.set_keys(field.get_ref_table(), [v[field.get_ref_field()] for v in results[0]])
        keys = KEY_REGISTER.get_keys(field.get_ref_table())
    return keys


async def generate_single_row(table: Table):
    """
    TODO: move to generator
    :param table:
    :return:
    """
    values = []
    for field_name in table.fields:
        field = table.fields[field_name]
        dtype = field.type
        value = None
        if dtype == DataTypeEnum.INT:
            value = random.randint(0, 10000)
        if dtype == DataTypeEnum.SERIAL:
            value = random.randint(0, 10000)
        if dtype == DataTypeEnum.UUID:
            value = uuid.uuid4()
        if dtype == DataTypeEnum.NUMERIC:
            value = random.random() * 10
        if dtype == DataTypeEnum.VARCHAR:
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k=int(10)))
        if dtype == DataTypeEnum.TIMESTAMP:
            value = datetime.datetime.now()

        if field.key == KeyTypeEnum.FOREIGN_KEY:
            keys = await get_foreign_key_values(field)
            total_len = len(keys)
            value = keys[random.randint(0, total_len - 1)]

        values.append(value)
    return values


async def generate_data_from_types(table: Table, size=10):
    """
    TODO: part of Generator class
    :param table:
    :param size:
    :return:
    """
    values = []
    for i in range(size):
        data = await generate_single_row(table)
        values.append(data)
    return values


async def create_insert_queries(table: Table, values: list) -> str:
    """
    TODO: make it readable for normal human beings, part of Generator class
    :param table_name:
    :param schema:
    :param values:
    :return:
    """
    parts = [f"INSERT INTO {table.name}("]
    field_list = []
    for col_name in table.fields:
        field_list.append(table.fields[col_name])
        parts.append(f'{col_name}, ')
    parts[-1] = parts[-1].strip(", ")
    parts.append(") VALUES (")
    for item in values:
        for i, column_val in enumerate(item):
            col_type = field_list[i].type
            if col_type == DataTypeEnum.INT:
                parts.append(f"{column_val}, ")
            else:
                parts.append(f"'{column_val}', ")
        parts[-1] = parts[-1].strip(", ")
        parts.append("), (")
    parts[-1] = parts[-1].strip(", (")
    parts.append(";")
    return "".join(parts)


async def insert_generate(filename: str | None):
    """
    TODO: order should be figured out by topological sort
    :return:
    """
    qs = await QueryFinder(filename).get_queries()
    parser = SQLParser()
    for command in qs:
        job_table: JobTable = parser.parse(command)
        if job_table.job != CommandTypeEnum.CREATE:
            continue

        print("Inserting for:", job_table.table.name)
        data = await generate_data_from_types(job_table.table, 3)
        query = await create_insert_queries(job_table.table, data)
        await run_execute([query])


async def provision(filename: str = None):
    print(filename)
    qf = QueryFinder(filename)
    qs = await qf.get_queries()
    await run_execute(qs)


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
        await insert_generate(args.file)
    else:
        await provision(args.file)


if __name__ == "__main__":
    asyncio.run(main())
