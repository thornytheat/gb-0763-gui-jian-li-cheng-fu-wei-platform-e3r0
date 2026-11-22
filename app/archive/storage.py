from contextlib import contextmanager
from pathlib import Path
import sqlite3


class TrackArchive:
    def __init__(self, filename):
        self.file = Path(filename)
        self.file.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, payload BLOB NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS issues (run_id TEXT PRIMARY KEY REFERENCES runs(id), seal TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS messages ('
                       'id TEXT NOT NULL, channel TEXT NOT NULL, seq INTEGER NOT NULL, '
                       'payload BLOB NOT NULL, detail TEXT NOT NULL, '
                       'PRIMARY KEY (channel, id), UNIQUE (channel, seq))')
            db.execute('CREATE TABLE IF NOT EXISTS resets ('
                       'id TEXT PRIMARY KEY, channel TEXT NOT NULL, seq INTEGER NOT NULL, '
                       'message_ids TEXT NOT NULL, payload TEXT NOT NULL)')
            db.execute('CREATE INDEX IF NOT EXISTS idx_resets_channel ON resets(channel, seq)')
            db.execute('CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, seq)')

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.file, timeout=15)
        try:
            db.execute('PRAGMA foreign_keys=ON')
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def write_transaction(self):
        """立即取写锁，避免并发写入时唯一约束竞争产生半批。"""
        db = sqlite3.connect(self.file, timeout=15)
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
