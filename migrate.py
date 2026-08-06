#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate.py - 数据迁移工具
支持 export / import / verify / list
"""
import sys
import os
import io
import json
import hashlib
import shutil
import tarfile
import tempfile
from pathlib import Path
from datetime import datetime
import argparse

# 确保 stdout 使用 UTF-8 编码（解决 Windows GBK 编码问题）
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).resolve().parent

# 默认要打包的路径
DEFAULT_PATHS = [
    "bot.py", "web.py", "templates.html",
    "migrate.py", "install.sh", "start.sh", "stop.sh", "run_tests.sh",
    "requirements.txt", "README.md",
    "data/", "_versions/", "config.json", ".env",
]

# 排除规则
EXCLUDE_PATTERNS = [
    ".deps", "__pycache__", "*.pyc", "*.swp", "*.swo",
    ".git", ".DS_Store", "data/logs/bot.out", "data/logs/bot.err",
    "data/web.pid", "data/bot.pid", "data/.restart_requested",
]


def should_exclude(p: Path) -> bool:
    s = str(p)
    for pat in EXCLUDE_PATTERNS:
        if pat.startswith("*"):
            if s.endswith(pat[1:]):
                return True
        elif pat in s:
            return True
    return False


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def cmd_export(args):
    """导出"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = args.output or f"sunset-bot-export-{timestamp}.tar.gz"
    out_path = Path(out_name).resolve()
    if not out_path.is_absolute():
        out_path = Path.home() / out_path

    paths = args.paths if args.paths else DEFAULT_PATHS
    with tarfile.open(out_path, "w:gz") as tar:
        for p in paths:
            src = BASE_DIR / p
            if not src.exists():
                print(f"  跳过（不存在）: {p}")
                continue
            if should_exclude(src):
                print(f"  跳过（排除）: {p}")
                continue
            tar.add(src, arcname=f"sunset-bot/{p}")
            print(f"  ✓ {p}")

    size_mb = out_path.stat().st_size / 1024 / 1024
    sha = compute_sha256(out_path)
    sha_path = out_path.with_suffix(out_path.suffix + ".sha256")
    sha_path.write_text(f"{sha}  {out_path.name}\n")

    print(f"\n✓ 已导出: {out_path} ({size_mb:.2f} MB)")
    print(f"✓ SHA256: {sha_path}")
    print(f"  {sha}")
    return True


def cmd_verify(args):
    """校验"""
    tar_path = Path(args.tar)
    if not tar_path.exists():
        print(f"✗ 文件不存在: {tar_path}")
        return False

    # SHA256 校验（如果存在）
    sha_path = tar_path.with_suffix(tar_path.suffix + ".sha256")
    if sha_path.exists():
        expected = sha_path.read_text().strip().split()[0]
        actual = compute_sha256(tar_path)
        if expected == actual:
            print(f"✓ SHA256 匹配: {expected[:16]}...")
        else:
            print(f"✗ SHA256 不匹配")
            print(f"  预期: {expected}")
            print(f"  实际: {actual}")
            if not args.force:
                return False

    # 解压测试 + 必需文件检查
    REQUIRED = {"bot.py", "web.py", "templates.html"}
    found = set()
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            for m in tar.getmembers():
                if not m.isfile():
                    continue
                name = m.name
                if name.startswith("sunset-bot/"):
                    name = name[len("sunset-bot/"):]
                if "/" in name:
                    name = name.split("/")[0]
                found.add(name)
    except tarfile.TarError as e:
        print(f"✗ tar 损坏: {e}")
        return False

    missing = REQUIRED - found
    if missing:
        print(f"✗ 缺少必需文件: {missing}")
        return False

    print(f"✓ 校验通过 (含 {len(found)} 个条目)")
    return True


def cmd_import(args):
    """导入"""
    if not cmd_verify(args):
        print("✗ 校验失败，未导入")
        return False

    tar_path = Path(args.tar)

    # 备份当前
    backup_dir = BASE_DIR / "_import_backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir.mkdir(parents=True, exist_ok=True)
    for p in DEFAULT_PATHS:
        src = BASE_DIR / p
        if src.exists() and not should_exclude(src):
            dst = backup_dir / p
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.is_file():
                shutil.copy2(src, dst)
            elif src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
    print(f"✓ 备份到 {backup_dir}")

    # 解压到临时目录，再覆盖 BASE_DIR
    tmp_extract = Path(tempfile.mkdtemp(prefix="sunset_import_"))
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            # 安全检查
            for m in tar.getmembers():
                if not (m.name == "sunset-bot" or m.name.startswith("sunset-bot/")):
                    print(f"✗ 危险路径: {m.name}")
                    return False
            # Python 3.12+ 使用 data filter 防止路径穿越
            import sys as _sys
            if _sys.version_info >= (3, 12):
                tar.extractall(tmp_extract, filter='data')
            else:
                tar.extractall(tmp_extract)
        # 复制 sunset-bot/* 到 BASE_DIR
        src_dir = tmp_extract / "sunset-bot"
        if not src_dir.exists():
            print(f"✗ tar 中无 sunset-bot/ 目录")
            return False
        for item in src_dir.iterdir():
            dst = BASE_DIR / item.name
            if item.is_dir():
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(item, dst)
            else:
                shutil.copy2(item, dst)
    except tarfile.TarError as e:
        print(f"✗ 解压失败: {e}")
        return False
    finally:
        shutil.rmtree(tmp_extract, ignore_errors=True)

    # 给 shell 脚本加可执行
    for sh in ["install.sh", "start.sh", "stop.sh", "run_tests.sh"]:
        sh_path = BASE_DIR / sh
        if sh_path.exists():
            os.chmod(sh_path, 0o755)

    print(f"✓ 已导入到 {BASE_DIR}")
    print(f"\n下一步:")
    print(f"  ./start.sh    # 启动")
    print(f"  或 bash install.sh  # 完整安装")
    return True


def cmd_list(args):
    """列出 tar 内文件"""
    tar_path = Path(args.tar)
    if not tar_path.exists():
        print(f"✗ 文件不存在: {tar_path}")
        return False
    with tarfile.open(tar_path, "r:gz") as tar:
        for m in tar.getmembers():
            size = m.size if m.isfile() else 0
            kind = "d" if m.isdir() else "-"
            print(f"  {kind} {size:>10}  {m.name}")
    return True


def main():
    parser = argparse.ArgumentParser(description="sunset-bot 数据迁移")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export", help="导出")
    p_export.add_argument("-o", "--output", help="输出文件名")
    p_export.add_argument("paths", nargs="*", help="要打包的路径 (默认全套)")
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser("import", help="导入")
    p_import.add_argument("tar", help="tar 文件")
    p_import.set_defaults(func=cmd_import)

    p_verify = sub.add_parser("verify", help="校验")
    p_verify.add_argument("tar", help="tar 文件")
    p_verify.add_argument("-f", "--force", action="store_true", help="SHA256 不匹配也通过")
    p_verify.set_defaults(func=cmd_verify)

    p_list = sub.add_parser("list", help="列出")
    p_list.add_argument("tar", help="tar 文件")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    if hasattr(args, "func"):
        ok = args.func(args)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
