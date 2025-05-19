import pytest, shutil, os
from pathlib import Path
from dlc.models import DlcCase

# use a temp DB per test
@pytest.fixture(autouse=True)
def tmp_db(monkeypatch, tmp_path):
    from dlc import storage
    monkeypatch.setattr(storage, "_DB_PATH", tmp_path / "db.sqlite")
    yield
    # no cleanup needed; tmp_path is removed

def test_only_one_active():
    # first case
    c1 = DlcCase(
        None, "n1", "detected", "sda", 1, "c1", 10, "/mnt"
    )
    c1.save()
    # second case with SAME osd_id/cluster should fail
    with pytest.raises(Exception):
        c2 = DlcCase(
            None, "n2", "detected", "sdb", 1, "c1", 9, "/mnt2"
        )
        c2.save()
    
    with pytest.raises(ValueError, match="Case not found"):
        DlcCase.load(999999999)

