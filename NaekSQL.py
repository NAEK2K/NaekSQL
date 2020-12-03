import sqlite3
import os

# Table Dictionary Format
# {"table_name": {"type": "type", "params": "params", "references": "table.column"}}


class NaekSQL:
    def __init__(self, db_name, **options):
        if options.get("fresh"):
            if os.path.exists(db_name):
                os.remove(db_name)
        self.db_name = db_name
        self.connect()
        self.disconnect()

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn.cursor()

    def disconnect(self):
        self.conn.commit()
        self.conn.close()

    def create_tables(self, tables):
        if not tables:
            return

        cursor = self.connect()

        for table, columns in tables.items():
            table_columns = []
            foreign_keys = []

            for column, settings in columns.items():
                table_column = [column]
                table_column.append(settings.get("type", "text"))

                if settings.get("params"):
                    table_column.append(settings.get("params"))

                if settings.get("references"):
                    foreign_key = settings.get("references").split(".")
                    if len(foreign_key) == 2:
                        foreign_keys.append(
                            "FOREIGN KEY ({}) REFERENCES {} ({})".format(
                                column, foreign_key[0], foreign_key[1]
                            )
                        )
                table_columns.append(" ".join(table_column))

            table_columns = ", ".join(table_columns)
            foreign_keys = ", ".join(foreign_keys)

            if foreign_keys:
                table_columns += ", {}".format(foreign_keys)

            sql_statement = "CREATE TABLE IF NOT EXISTS {} ({})".format(
                table, table_columns
            )
            cursor.execute(sql_statement)
        self.disconnect()
