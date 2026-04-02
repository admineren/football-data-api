import asyncio
import asyncpg
import csv
import os
import sys

# 🔥 ENV'den al
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL bulunamadı!")

len(sys.argv) < 2:
    print("Kullanım: python insert.py dosya.csv")
    exit()

CSV_FILE = sys.argv[1]


# 🧠 güvenli dönüşümler
def safe_int(val):
    return int(val) if val not in ("", None) else None


def safe_float(val):
    return float(val) if val not in ("", None) else None


def safe_bool(val):
    return val == "True"


async def insert_data():
    conn = await asyncpg.connect(DATABASE_URL)

    inserted = 0
    skipped = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                await conn.execute("""
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
                """,

                row["match_id"],
                row["country"],
                row["league"],
                row["season"],
                row["home_team"],
                row["away_team"],
                row["date"],
                row["time"],

                safe_int(row["ht_home"]),
                safe_int(row["ht_away"]),
                safe_int(row["ft_home"]),
                safe_int(row["ft_away"]),

                safe_int(row["ht_total_goals"]),
                safe_int(row["ft_total_goals"]),
                safe_int(row["goal_diff"]),
                row["result"],
                row["ht_ft"],
                safe_bool(row["has_odds"]),

                row["bookmaker_1x2"],
                safe_float(row["home_odds"]),
                safe_float(row["draw_odds"]),
                safe_float(row["away_odds"]),

                row["bookmaker_ou1.5"],
                safe_float(row["ou1.5_over"]),
                safe_float(row["ou1.5_under"]),
                row["bookmaker_ou2.5"],
                safe_float(row["ou2.5_over"]),
                safe_float(row["ou2.5_under"]),
                row["bookmaker_ou3.5"],
                safe_float(row["ou3.5_over"]),
                safe_float(row["ou3.5_under"]),
                row["bookmaker_ou4.5"],
                safe_float(row["ou4.5_over"]),
                safe_float(row["ou4.5_under"]),

                row["bookmaker_btts"],
                safe_float(row["btts_yes"]),
                safe_float(row["btts_no"]),

                row["bookmaker_ah"],
                row["ah_line"],
                safe_float(row["ah_home"]),
                safe_float(row["ah_away"]),
                )

                inserted += 1

            except Exception as e:
                skipped += 1
                print("HATALI SATIR:", e)

    await conn.close()

    print(f"Inserted: {inserted}")
    print(f"Skipped: {skipped}")


# 🚀 çalıştır
asyncio.run(insert_data())
