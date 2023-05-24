import os
import itertools


class QueryFinder:

    def __init__(self, path: str | None):
        self.path = path or '.'

    def _get_sql_files(self):
        files = []
        for dirpath, dirnames, filenames in os.walk(self.path):
            for filename in filenames:
                if filename.endswith(".sql"):
                    files.append(os.path.join(dirpath, filename))
        return files

    def _get_queries_from_single_file(self, filename: str):
        print(f"Checking {filename}")
        with open(filename, 'r') as rd:
            content = rd.read()
            qs = [q for q in content.split("\n\n") if q]
            return qs

    async def _get_queries_from_files(self, files: list[str]) -> list[str]:
        q = [self._get_queries_from_single_file(f) for f in files]
        return list(itertools.chain(*q))

    async def get_queries(self):
        files = self._get_sql_files()
        qs = await self._get_queries_from_files(files)
        return qs
