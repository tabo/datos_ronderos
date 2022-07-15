import sqlite3

from candidatos import MAIN_DIR


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(MAIN_DIR / "candidatos.db")

    def get_table_and_key(self, caca):
        pass


db = Database()
