import enum
import re


class CommandTypeEnum(enum.Enum):
    ALTER = "alter"  # NOT SUPPORTED
    DROP = "drop"
    CREATE = "create"


class DataTypeEnum(enum.Enum):
    INT = "int"
    SERIAL = "serial"
    UUID = "uuid"
    NUMERIC = "numeric"
    VARCHAR = "varchar"
    FOREIGN_KEY = "fkey"
    TIMESTAMP = "timestamp"


class KeyTypeEnum(enum.Enum):
    FOREIGN_KEY = "fkey"
    PRIMARY_KEY = "pkey"


class DataType:

    def __init__(self, name: str, dtype: DataTypeEnum, is_null: bool = True):
        self.name = name
        self.type = dtype
        # TODO: support -> self.parameter = parameter
        self.key = None
        self._key_table = None
        self._key_table_field = None
        self.is_null = is_null

    def __repr__(self):
        key = f',{self.key}' if self.key else ''
        return f'<Field:{self.name},{self.type}{key}>'

    def __str__(self):
        return self.__repr__()

    def set_primary(self):
        self.key = KeyTypeEnum.PRIMARY_KEY

    def set_foreign(self, referenced_table: str, referenced_field: str):
        self.key = KeyTypeEnum.FOREIGN_KEY
        self._key_table = referenced_table
        self._key_table_field = referenced_field

    def get_select_for_foreign_key(self):
        return f"SELECT {self._key_table_field} FROM {self._key_table} LIMIT 1000;"

    def get_ref_field(self):
        return self._key_table_field

    def get_ref_table(self):
        return self._key_table


class Table:

    def __init__(self, name: str, fields: dict[str, DataType]):
        self.name = name
        self.fields = fields

    def __repr__(self):
        fields = [self.fields[k] for k in self.fields]
        return f'Table({self.name})-->Fields:{fields}'

    def __str__(self):
        return self.__repr__()


class JobTable:
    def __init__(self, table: Table, job: CommandTypeEnum):
        self.job = job
        self.table = table


class Parser:

    def parse(self, command: str):
        return


class SQLParser(Parser):
    """
    Returns schema with python types.
    """

    def __init__(self):
        pass

    def _get_parts(self, command: str) -> dict:
        """
        TODO: add some command validation, use less strip/split, use stack solution instead?
        :param command:
        :return:
        """
        megaline = "".join(command.split("\n"))
        parts = [part.strip() for part in megaline.split("(", 1)]
        if len(parts) == 1:
            return {"header": parts[0].strip(";"), "fields": []}
        header = parts[0]
        fields = [part.strip("; ") for part in re.split(r',[\t\n\r\s]+', parts[1])]
        return {
            "header": header,
            "fields": fields
        }

    def _get_command_type(self, line: str) -> CommandTypeEnum:
        command_name = line.lower().strip().split(" ")[0]
        return CommandTypeEnum(command_name)

    def _get_table_name(self, line: str):
        return line.lower().strip().strip(";").split(" ")[-1]

    def _header(self, line: str) -> tuple[CommandTypeEnum, str]:
        command_type = self._get_command_type(line)
        table_name = self._get_table_name(line)
        return command_type, table_name

    def _handle_primary_key(self, line: str) -> str:
        return line.split("(")[-1].strip("), ")

    def _parse_dtype(self, dtype: str):
        return dtype.split("(")[0]

    def _handle_foreign_key(self, line: str) -> tuple[str, str, str]:
        field_patt = re.compile("\(.*?\)")
        ref_table_patt = re.compile('".*?"')
        fields = re.findall(field_patt, line)
        ref = re.findall(ref_table_patt, line)
        local_column, referenced_column = fields
        return local_column.strip("()"), ref[0].strip('"'), referenced_column.strip("()")

    def _parse_column(self, so_far_dtypes: dict[str, DataType], line: str) -> DataType | None:
        lower_line = line.lower().strip()
        # indexes
        if lower_line.startswith("primary key"):
            column_name = self._handle_primary_key(lower_line)
            so_far_dtypes[column_name].set_primary()
            return
        if lower_line.startswith("foreign key"):
            column_name, referenced_table, referenced_field = self._handle_foreign_key(lower_line)
            so_far_dtypes[column_name].set_foreign(referenced_table, referenced_field)
            return

        # fields
        is_null = True
        if "not null" in lower_line:
            is_null = False
        name, type, *rest = lower_line.split()
        dtype = DataType(name, DataTypeEnum(self._parse_dtype(type)), is_null)
        if "primary" in lower_line:
            dtype.set_primary()
        return dtype

    def _fields(self, lines: list[str]) -> dict[str, DataType]:
        fields = {}
        for line in lines:
            dtype = self._parse_column(fields, line)
            if dtype:
                fields[dtype.name] = dtype
        return fields

    def parse(self, command: str) -> JobTable:
        parts = self._get_parts(command)
        command_type, table_name = self._header(parts["header"])
        fields = self._fields(parts["fields"])
        return JobTable(Table(table_name, fields), command_type)


def test_create_table():
    sql = """CREATE TABLE IF NOT EXISTS Location (
            id serial PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            address VARCHAR NOT NULL
        );"""
    job_table = SQLParser().parse(sql)
    print(job_table.table)


def test_create_table_foreign_key():
    sql = """CREATE TABLE IF NOT EXISTS Sensor (
        id uuid DEFAULT gen_random_uuid (),
        location_id int,
        type VARCHAR NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (location_id) REFERENCES "location" (id)
    );"""
    job_table = SQLParser().parse(sql)
    print(job_table.table)


if __name__ == "__main__":
    test_create_table()
    test_create_table_foreign_key()