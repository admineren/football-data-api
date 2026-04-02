import psycopg2
import csv
import os

DB_URL = os.getenv("DATABASE_URL")

BATCH_SIZE = 500


def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")


def insert_batch(cur, rows):
    query = """
    INSERT INTO matches (
        match_id,country,league,season,
        home_team,away_team,date,time,
        ht_home,ht_away,ft_home,ft_away,
        ht_total_goals,ft_total_goals,goal_diff,
        result,ht_ft,has_odds,
        bookmaker_1x2,home_odds,draw_odds,away_odds,
        bookmaker_ou1_5,ou1_5_over,ou1_5_under,
        bookmaker_ou2_5,ou2_5_over,ou2_5_under,
        bookmaker_ou3_5,ou3_5_over,ou3_5_under,
        bookmaker_ou4_5,ou4_5_over,ou4_5_under,
        bookmaker_btts,btts_yes,btts_no,
        bookmaker_ah,ah_line,ah_home,ah_away
    ) VALUES %s
    ON CONFLICT (match_id) DO NOTHING;
    """

    from psycopg2.extras import execute_values
    execute_values(cur, query, rows)


def parse_row(row):
    try:
        return (
            row["match_id"],
            row["country"],
            row["league"],
            row["season"],
            row["home_team"],
            row["away_team"],
            row["date"],
            row["time"],

            int(row["ht_home"]) if row["ht_home"] else None,
            int(row["ht_away"]) if row["ht_away"] else None,
            int(row["ft_home"]) if row["ft_home"] else None,
            int(row["ft_away"]) if row["ft_away"] else None,

            int(row["ht_total_goals"]) if row["ht_total_goals"] else None,
            int(row["ft_total_goals"]) if row["ft_total_goals"] else None,
            int(row["goal_diff"]) if row["goal_diff"] else None,

            row["result"],
            row["ht_ft"],
            row["has_odds"] == "True",

            row["bookmaker_1x2"],
            float(row["home_odds"]) if row["home_odds"] else None,
            float(row["draw_odds"]) if row["draw_odds"] else None,
            float(row["away_odds"]) if row["away_odds"] else None,

            row["bookmaker_ou1.5"],
            float(row["ou1.5_over"]) if row["ou1.5_over"] else None,
            float(row["ou1.5_under"]) if row["ou1.5_under"] else None,

            row["bookmaker_ou2.5"],
            float(row["ou2.5_over"]) if row["ou2.5_over"] else None,
            float(row["ou2.5_under"]) if row["ou2.5_under"] else None,

            row["bookmaker_ou3.5"],
            float(row["ou3.5_over"]) if row["ou3.5_over"] else None,
            float(row["ou3.5_under"]) if row["ou3.5_under"] else None,

            row["bookmaker_ou4.5"],
            float(row["ou4.5_over"]) if row["ou4.5_over"] else None,
            float(row["ou4.5_under"]) if row["ou4.5_under"] else None,

            row["bookmaker_btts"],
            float(row["btts_yes"]) if row["btts_yes"] else None,
            float(row["btts_no"]) if row["btts_no"] else None,

            row["bookmaker_ah"],
            row["ah_line"],
            float(row["ah_home"]) if row["ah_home"] else None,
            float(row["ah_away"]) if row["ah_away"] else None,
        )

    except Exception:
        return None


def load_csv(file_path):
    conn = get_conn()
    cur = conn.cursor()

    batch = []
    inserted = 0
    skipped = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            parsed = parse_row(row)

            if not parsed:
                skipped += 1
                continue

            batch.append(parsed)

            if len(batch) >= BATCH_SIZE:
                try:
                    insert_batch(cur, batch)
                    conn.commit()
                    inserted += len(batch)
                except:
                    conn.rollback()
                batch = []

        if batch:
            insert_batch(cur, batch)
            conn.commit()
            inserted += len(batch)

    cur.close()
    conn.close()

    print(f"Inserted: {inserted}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    load_csv("matches.csv")
