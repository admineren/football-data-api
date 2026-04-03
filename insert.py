import asyncio
import asyncpg
import csv
import os
import sys
from datetime import datetime
from glob import glob
from tqdm import tqdm
import time

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL bulunamadı!")

if len(sys.argv) < 2:
    print("Kullanım:")
    print("python insert.py file.csv")
    print("python insert.py klasor/")
    exit()

TARGET = sys.argv[1]


# 🔒 dönüşümler
def safe_int(val):
    try:
        return int(val) if val not in ("", None) else None
    except:
        return None


def safe_float(val):
    try:
        return float(val) if val not in ("", None) else None
    except:
        return None


def safe_bool(val):
    return str(val).lower() == "true"


def safe_date(val):
    try:
        return datetime.strptime(val, "%Y-%m-%d").date() if val else None
    except:
        return None


# 🔥 row → tuple
def transform(row):
    return (
        row.get("match_id"),
        row.get("country"),
        row.get("league"),
        safe_int(row.get("season")),

        row.get("home_team"),
        row.get("away_team"),
        safe_date(row.get("date")),
        row.get("time") or None,

        safe_int(row.get("ht_home")),
        safe_int(row.get("ht_away")),
        safe_int(row.get("ft_home")),
        safe_int(row.get("ft_away")),

        safe_int(row.get("ht_total_goals")),
        safe_int(row.get("ft_total_goals")),
        safe_int(row.get("goal_diff")),
        row.get("result"),
        row.get("ht_ft"),
        safe_bool(row.get("has_odds")),

        row.get("bookmaker_1x2"),
        safe_float(row.get("home_odds")),
        safe_float(row.get("draw_odds")),
        safe_float(row.get("away_odds")),

        row.get("bookmaker_ou1.5"),
        safe_float(row.get("ou1.5_over")),
        safe_float(row.get("ou1.5_under")),
        row.get("bookmaker_ou2.5"),
        safe_float(row.get("ou2.5_over")),
        safe_float(row.get("ou2.5_under")),
        row.get("bookmaker_ou3.5"),
        safe_float(row.get("ou3.5_over")),
        safe_float(row.get("ou3.5_under")),
        row.get("bookmaker_ou4.5"),
        safe_float(row.get("ou4.5_over")),
        safe_float(row.get("ou4.5_under")),

        row.get("bookmaker_btts"),
        safe_float(row.get("btts_yes")),
        safe_float(row.get("btts_no")),

        row.get("bookmaker_ah"),
        row.get("ah_line"),
        safe_float(row.get("ah_home")),
        safe_float(row.get("ah_away")),
    )


INSERT_SQL = """
INSERT INTO matches (
    match_id, country, league, season,
    home_team, away_team, date, time,
    ht_home, ht_away, ft_home, ft_away,
    ht_total_goals, ft_total_goals,
    goal_diff, result, ht_ft, has_odds,
    bookmaker_1x2, home_odds, draw_odds, away_odds,
    bookmaker_ou1_5, ou1_5_over, ou1_5_under,
    bookmaker_ou2_5, ou2_5_over, ou2_5_under,
    bookmaker_ou3_5, ou3_5_over, ou3_5_under,
    bookmaker_ou4_5, ou4_5_over, ou4_5_under,
    bookmaker_btts, btts_yes, btts_no,
    bookmaker_ah, ah_line, ah_home, ah_away
)
VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,
    $9,$10,$11,$12,
    $13,$14,$15,$16,$17,$18,
    $19,$20,$21,$22,
    $23,$24,$25,$26,$27,$28,
    $29,$30,$31,$32,$33,$34,
    $35,$36,$37,
    $38,$39,$40,$41
)
ON CONFLICT (match_id) DO NOTHING
"""


# 🚀 FILE PROCESS
async def process_file(pool, file_path, batch_size=500):
    inserted = 0
    skipped = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        total = len(reader)

    print(f"\n📂 FILE: {file_path} | Rows: {total}")

    start = time.time()
    batch = []

    async with pool.acquire() as conn:
        for i, row in enumerate(tqdm(reader, desc="Importing", unit="rows")):
            try:
                batch.append(transform(row))

                if len(batch) >= batch_size:
                    await conn.executemany(INSERT_SQL, batch)
                    inserted += len(batch)
                    batch.clear()

            except Exception as e:
                skipped += 1
                print("❌ ERROR:", e)

            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                eta = (total - (i + 1)) / rate if rate > 0 else 0

                print(f"\n📊 {i+1}/{total} | {rate:.1f} row/s | ETA: {eta:.1f}s")

        if batch:
            await conn.executemany(INSERT_SQL, batch)
            inserted += len(batch)

    print(f"✅ DONE: {file_path}")
    print(f"Inserted: {inserted}, Skipped: {skipped}")


# 🚀 MAIN
async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    if os.path.isfile(TARGET):
        files = [TARGET]
    else:
        files = sorted(glob(os.path.join(TARGET, "*.csv")))

    if not files:
        print("❌ CSV bulunamadı!")
        return

    print(f"\n🚀 TOTAL FILES: {len(files)}")

    for file in files:
        await process_file(pool, file)

    await pool.close()
    print("\n🎯 ALL DONE")


asyncio.run(main())
