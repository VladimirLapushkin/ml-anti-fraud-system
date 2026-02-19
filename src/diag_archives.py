#!/usr/bin/env python3
import os, sys, subprocess

def sh(cmd):
    print("\n$ "+cmd, flush=True)
    p = subprocess.run(["bash","-lc",cmd], text=True, capture_output=True)
    print(p.stdout, end="", flush=True); print(p.stderr, end="", flush=True)

sh("pwd; ls -la")
sh("ls -la .venv || true; ls -la .venv/bin || true")
sh("env | grep -E 'PYSPARK|SPARK|YARN' | sort | sed -n '1,200p'")

def sh(cmd: str):
    print("\n$ " + cmd, flush=True)
    p = subprocess.run(["bash", "-lc", cmd], text=True, capture_output=True)
    if p.stdout:
        print(p.stdout, end="", flush=True)
    if p.stderr:
        print(p.stderr, end="", flush=True)

print("sys.executable:", sys.executable, flush=True)
print("cwd:", os.getcwd(), flush=True)
print("PWD env:", os.environ.get("PWD"), flush=True)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("diag-archives").getOrCreate()
print("Spark version:", spark.version, flush=True)

# принудительно создаём job, чтобы Spark поднял executor ---
print("Triggering a tiny Spark job to force executors...", flush=True)
cnt = spark.range(1).count()
print("spark.range(1).count() =", cnt, flush=True)


sh("pwd; ls -la")

# Проверим: .venv распакован
sh("ls -la .venv || true")
sh("stat .venv || true; readlink -f .venv || true")
sh("ls -la .venv/bin || true")


sh(r"find .venv -maxdepth 4 -type f \\( -name python -o -name python3 -o -name python3.9 -o -name pyvenv.cfg \\) -print | sed -n '1,200p'")


sh("command -v file >/dev/null 2>&1 && file .venv/bin/python || true")
sh("command -v readelf >/dev/null 2>&1 && readelf -l .venv/bin/python | grep -i interpreter || true")
sh("command -v ldd >/dev/null 2>&1 && ldd .venv/bin/python || true")

# версия питона внутри venv (если запускается)
sh(".venv/bin/python -V || true")
sh(".venv/bin/python -c \"import sys; print('exe=', sys.executable); print('prefix=', sys.prefix); print('base_prefix=', getattr(sys,'base_prefix', None))\" || true")

spark.stop()
print("DONE", flush=True)
