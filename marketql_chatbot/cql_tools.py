from typing import List
import pandas as pd

class CassandraDatabase:
    def __init__(self, session, keyspace: str):
        self.session = session
        self.keyspace = keyspace

    def get_table_names(self) -> List[str]:
        rows = self.session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s",
            [self.keyspace]
        )
        return [r.table_name for r in rows]

    def get_table_schema(self, table: str) -> str:
        rows = self.session.execute(
            "SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s",
            [self.keyspace, table]
        )
        info = []
        for r in rows:
            tag = " (PARTITION KEY)" if r.kind == 'partition_key' else (" (CLUSTERING KEY)" if r.kind == 'clustering' else "")
            info.append(f"- {r.column_name} {r.type}{tag}")
        return f"Table {table}:\n" + "\n".join(info) if info else f"Table {table}: schema unavailable"

    def get_database_info(self) -> str:
        tables = self.get_table_names()
        out = [f"Keyspace: {self.keyspace}", f"Tables ({len(tables)}): {', '.join(tables) or '(none)'}", ""]
        for t in tables[:5]:
            out.append(self.get_table_schema(t))
            out.append("")
        if len(tables) > 5:
            out.append(f"... and {len(tables)-5} more")
        return "\n".join(out)

    def execute_cql(self, cql: str) -> str:
        cql = cql.strip().rstrip(";")
        try:
            rows = self.session.execute(cql)
            if hasattr(rows, 'column_names'):
                cols = rows.column_names
                data = []
                for row in rows:
                    if hasattr(row, "_asdict"):
                        data.append(row._asdict())
                    else:
                        data.append({c: getattr(row, c) for c in cols})
                if not data:
                    return "No results."
                df = pd.DataFrame(data)
                return df.to_string(index=False)
            return "Query executed."
        except Exception as e:
            return f"Error: {e}"
