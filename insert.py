from psycopg2.extras import execute_values

def insert_matches(conn, rows):
    cur = conn.cursor()

    query = """
    INSERT INTO matches (
        match_id, country, league, season,
        home_team, away_team, date, time,
        ht_home, ht_away, ft_home, ft_away,
        ht_total_goals, ft_total_goals, goal_diff,
        result, ht_ft, has_odds,

        bookmaker_1x2, home_odds, draw_odds, away_odds,

        bookmaker_ou1_5, ou1_5_over, ou1_5_under,
        bookmaker_ou2_5, ou2_5_over, ou2_5_under,
        bookmaker_ou3_5, ou3_5_over, ou3_5_under,
        bookmaker_ou4_5, ou4_5_over, ou4_5_under,

        bookmaker_btts, btts_yes, btts_no,

        bookmaker_ah, ah_line, ah_home, ah_away
    )
    VALUES %s
    ON CONFLICT (match_id) DO NOTHING
    """

    values = [
        (
            r.get("match_id"), r.get("country"), r.get("league"), r.get("season"),
            r.get("home_team"), r.get("away_team"), r.get("date"), r.get("time"),
            r.get("ht_home"), r.get("ht_away"), r.get("ft_home"), r.get("ft_away"),
            r.get("ht_total_goals"), r.get("ft_total_goals"), r.get("goal_diff"),
            r.get("result"), r.get("ht_ft"), r.get("has_odds"),

            r.get("bookmaker_1x2"), r.get("home_odds"), r.get("draw_odds"), r.get("away_odds"),

            r.get("bookmaker_ou1.5"), r.get("ou1.5_over"), r.get("ou1.5_under"),
            r.get("bookmaker_ou2.5"), r.get("ou2.5_over"), r.get("ou2.5_under"),
            r.get("bookmaker_ou3.5"), r.get("ou3.5_over"), r.get("ou3.5_under"),
            r.get("bookmaker_ou4.5"), r.get("ou4.5_over"), r.get("ou4.5_under"),

            r.get("bookmaker_btts"), r.get("btts_yes"), r.get("btts_no"),

            r.get("bookmaker_ah"), r.get("ah_line"), r.get("ah_home"), r.get("ah_away"),
        )
        for r in rows
    ]

    execute_values(cur, query, values)
    conn.commit()
  
