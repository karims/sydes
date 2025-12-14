from pathlib import Path

from sydes.repo.scanner import scan_python_files


def test_scan_python_files_finds_src_files():
    repo_root = Path(__file__).resolve().parents[1]
    files = scan_python_files(repo_root, max_files=5000)

    target = (repo_root / "src" / "sydes" / "cli.py").resolve()
    assert any(Path(p).resolve() == target for p in files)
