#!/usr/bin/env python3
"""
generate_all_repos_raw_urls.py

Ambil semua file dari SELURUH repository milik satu user/org (contoh: uppermoon77),
buat URL raw GitHub untuk tiap file, lalu simpan ke CSV dan XLSX.

Contoh pakai (publik repos):
  python generate_all_repos_raw_urls.py uppermoon77 --out data/uppermoon77_all_raw.csv

Jika ingin sertakan private repos:
  export GITHUB_TOKEN="ghp_..."  # PAT scope: repo
  python generate_all_repos_raw_urls.py uppermoon77 --out data/uppermoon77_all_raw.csv --workers 10
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

GITHUB_API = "https://api.github.com"
PER_PAGE = 100


def github_get(url: str, token: Optional[str] = None, params: Dict[str, Any] | None = None, timeout: int = 30):
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, params=params, timeout=timeout)

    # Sederhana: handle rate limit (403 + remaining=0) -> tunggu sampai reset
    if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
        reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(5, reset - int(time.time()) + 3)
        print(f"[WARN] Rate limited. Tidur {wait} detik ...", file=sys.stderr)
        time.sleep(wait)
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
    return r


def list_repos_for_user(owner: str, token: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ambil daftar repos milik user/org (public jika tanpa token).
    """
    repos: List[Dict[str, Any]] = []
    page = 1
    while True:
        url = f"{GITHUB_API}/users/{owner}/repos"
        params = {"per_page": PER_PAGE, "page": page, "type": "all", "sort": "full_name", "direction": "asc"}
        r = github_get(url, token=token, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Gagal list repos {owner}: {r.status_code} {r.text}")
        items = r.json()
        if not items:
            break
        repos.extend(items)
        if len(items) < PER_PAGE:
            break
        page += 1
    return repos


def get_tree_recursive(owner: str, repo: str, branch: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    /repos/{owner}/{repo}/git/trees/{branch}?recursive=1
    """
    url = f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/{branch}"
    params = {"recursive": 1}
    r = github_get(url, token=token, params=params)
    if r.status_code == 404:  # fallback refs/heads/<branch>
        url2 = f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/refs/heads/{branch}"
        r = github_get(url2, token=token, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Gagal ambil tree {owner}/{repo}@{branch}: {r.status_code} {r.text}")
    return r.json()


def build_raw_rows_for_repo(owner: str, repo_item: Dict[str, Any], token: Optional[str] = None) -> List[Dict[str, Any]]:
    repo_name = repo_item.get("name")
    branch = repo_item.get("default_branch") or "main"
    try:
        tree_json = get_tree_recursive(owner, repo_name, branch, token=token)
    except Exception as e:
        print(f"[ERROR] {repo_name}: {e}", file=sys.stderr)
        return [{"repo": repo_name, "path": None, "size": None, "branch": branch, "raw_url": None, "error": str(e)}]

    rows: List[Dict[str, Any]] = []
    for node in tree_json.get("tree", []):
        if node.get("type") != "blob":
            continue
        path = node.get("path")
        size = node.get("size")
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo_name}/{branch}/{path}"
        rows.append({"repo": repo_name, "path": path, "size": size, "branch": branch, "raw_url": raw_url})
    return rows


def main():
    ap = argparse.ArgumentParser(description="Generate raw.githubusercontent.com URLs untuk seluruh repos milik user/org.")
    ap.add_argument("owner", help="Username/Org GitHub. Contoh: uppermoon77")
    ap.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub PAT (opsional). Bisa pakai env GITHUB_TOKEN")
    ap.add_argument("--out", default=None, help="Nama file output (.csv atau .xlsx). Default: <owner>-all-raw_urls.csv")
    ap.add_argument("--workers", type=int, default=6, help="Thread workers paralel (default 6)")
    ap.add_argument("--only-public", action="store_true", help="Paksa hanya public repos")
    args = ap.parse_args()

    owner = args.owner
    token = args.token
    out_name = args.out or f"{owner}-all-raw_urls.csv"

    print(f"Listing repos untuk {owner} ...")
    repos = list_repos_for_user(owner, token=token)
    if args.only_public:
        repos = [r for r in repos if not r.get("private", False)]
    print(f"Ditemukan {len(repos)} repo. Proses paralel {args.workers} workers ...")

    all_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futmap = {ex.submit(build_raw_rows_for_repo, owner, repo, token): repo for repo in repos}
        for fut in as_completed(futmap):
            repo = futmap[fut]
            try:
                rows = fut.result()
                if rows:
                    all_rows.extend(rows)
                print(f"[OK] {repo.get('name')}: {len(rows)} entri")
            except Exception as e:
                print(f"[ERR] {repo.get('name')}: {e}", file=sys.stderr)

    if not all_rows:
        print("Tidak ada file yang terkumpul. Keluar.")
        sys.exit(0)

    df = pd.DataFrame(all_rows)
    # Normalisasi kolom
    cols = ["repo", "path", "size", "branch", "raw_url"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    # Simpan CSV
    os.makedirs(os.path.dirname(out_name) or ".", exist_ok=True)
    csv_path = out_name if out_name.lower().endswith(".csv") else os.path.splitext(out_name)[0] + ".csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"Simpan CSV: {csv_path}")

    # Simpan XLSX juga
    xlsx_path = os.path.splitext(csv_path)[0] + ".xlsx"
    try:
        df.to_excel(xlsx_path, index=False)
        print(f"Simpan Excel: {xlsx_path}")
    except Exception as e:
        print("Peringatan: gagal menulis Excel:", e, file=sys.stderr)

    print("Selesai. Contoh URL:", df.iloc[0]["raw_url"])


if __name__ == "__main__":
    main()
