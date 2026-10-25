import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import migrate_storage
from services.storage.database_storage import DatabaseStorageBackend


class StorageMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.url = f"sqlite:///{self.root / 'target.db'}"
        self.accounts = [{"access_token": "test-at", "refresh_token": "test-rt"}]
        self.keys = [{"id": "test-key", "name": "test-user", "enabled": True}]
        (self.root / "accounts.json").write_text(json.dumps(self.accounts), encoding="utf-8")
        (self.root / "auth_keys.json").write_text(json.dumps({"items": self.keys}), encoding="utf-8")
        self.env = patch.dict(os.environ, DATABASE_URL=self.url, STORAGE_BACKEND="json")
        self.env.start()
        self.addCleanup(self.env.stop)
        self.data = patch.object(migrate_storage, "DATA_DIR", self.root)
        self.data.start()
        self.addCleanup(self.data.stop)

    def target(self):
        backend = DatabaseStorageBackend(self.url)
        self.addCleanup(backend.engine.dispose)
        return backend

    def test_migrates_accounts_and_auth_keys_and_restores_environment(self):
        migrate_storage.migrate_data("json", "sqlite")
        backend = self.target()
        self.assertEqual(backend.load_accounts(), self.accounts)
        self.assertEqual(backend.load_auth_keys(), self.keys)
        self.assertEqual(os.environ["STORAGE_BACKEND"], "json")

    def test_refuses_nonempty_target_without_overwriting_it(self):
        backend = self.target()
        original = [{"access_token": "existing-at"}]
        backend.save_accounts(original)
        with self.assertRaisesRegex(ValueError, "not empty"):
            migrate_storage.migrate_data("json", "sqlite")
        self.assertEqual(backend.load_accounts(), original)
        self.assertEqual(backend.load_auth_keys(), [])
        self.assertEqual(os.environ["STORAGE_BACKEND"], "json")

    def test_refuses_target_containing_only_auth_keys(self):
        backend = self.target()
        backend.save_auth_keys(self.keys)
        with self.assertRaisesRegex(ValueError, "not empty"):
            migrate_storage.migrate_data("json", "sqlite")
        self.assertEqual(backend.load_accounts(), [])
        self.assertEqual(backend.load_auth_keys(), self.keys)

    def test_corrupt_source_does_not_write_target(self):
        backend = self.target()
        (self.root / "auth_keys.json").write_text("{invalid", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            migrate_storage.migrate_data("json", "sqlite")
        self.assertEqual(backend.load_accounts(), [])
        self.assertEqual(backend.load_auth_keys(), [])
        self.assertEqual(os.environ["STORAGE_BACKEND"], "json")


if __name__ == "__main__":
    unittest.main()
