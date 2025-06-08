import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
import pandas as pd
import sqlite3
import streamlit as st

conn = sqlite3.connect('ipl_stats.db')
import logging

# Setup logging to a file
logging.basicConfig(filename='ipl_chatbot.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def ask_gemini_for_sql(question):
    prompt = f"""
You are an expert SQL query generator for an IPL cricket database with normalized tables.

Tables:
- ball_by_ball(
    season_id, match_id, batter, bowler, non_striker,
    team_batting, team_bowling, over_number, ball_number,
    batter_runs, extras, total_runs, batsman_type, bowler_type,
    player_out, fielders_involved, is_wicket, is_wide_ball, is_no_ball,
    is_leg_bye, is_bye, is_penalty, wide_ball_runs, no_ball_runs,
    leg_bye_runs, bye_runs, penalty_runs, wicket_kind, is_super_over, innings)
- match_data(
    match_id, season_id, balls_per_over, city, match_date, event_name,
    match_number, gender, match_type, format, overs, season,
    team_type, venue, toss_winner, team1, team2, toss_decision,
    match_winner, win_by_runs, win_by_wickets, player_of_match, result
)

- players(
    player_id, player_name, bat_style, bowl_style, field_pos, player_full_name
)

- teams(
    team_id, team_name
)

- team_aliases(
    alias_id, team_id, alias_name
)
Guidelines:
- To find award winners like orange cap (most runs in a season) or purple cap (most wickets in a season), query ball_by_ball grouped by player and season, ordered by runs or wickets desc with LIMIT 1.
- Use only the columns and table names as defined above.
- The `batter`, `bowler`, `player_out`, `fielders_involved` fields store **player names**, not IDs.
- Use JOINs with `players.player_name` when filtering or selecting players.
- Use `match_data.season` to filter by IPL season (e.g., 2020).
- Use `teams.team_id` to resolve team names in match_data fields like `team1`, `team2`, `toss_winner`, `match_winner`.
- Use the `team_aliases` table to match short forms (e.g., 'RCB', 'CSK') to full team names.
- For queries involving partial player names (e.g., "Dhoni"), use `LIKE '%Dhoni%'`. Use 'players.full_name'. 
- For strike rate calculations, use:  
  **balls faced** = COUNT of legitimate balls (excluding wides and no balls);  
  **strike rate** = (runs / balls faced) * 100  
  Only count deliveries where `is_wide_ball = 0 and is_no_ball=0`.
- For batting average: use (total runs) / (number of dismissals).
- For bowling strike rate: use (total legitimate balls bowled) / (number of wickets taken).
- For bowling average: use (total runs conceded) / (number of wickets taken).
- For catches: use `wicket_kind = 'caught'` and match `fielders_involved` with `players.player_name` using `LIKE`.
- For run outs: use `wicket_kind = 'run out'` and match `fielders_involved` with `players.player_name` using `LIKE`.
- Prefer SQL window functions (`RANK()`, `ROW_NUMBER()`, etc.) for ranking queries.
- Return only the final SQL query without explanation.
- Use match_data.player_of_match to find the player who won the Player of the Match award. This field contains the player ID, so JOIN with players table on players.player_id
- Matches in playoffs have match_data.match_number IS NULL
- Use match_data.season to filter by year or season range.
      Example for range:
      WHERE match_data.season BETWEEN 2018 AND 2020
- For stumpings: wicket_kind = 'stumped'
Examples:

Q: How many runs did Dhoni score in 2018?
A: SELECT SUM(b.batter_runs) AS total_runs
   FROM ball_by_ball b
   JOIN match_data m ON b.match_id = m.match_id
   JOIN players p ON b.batter = p.player_name
   WHERE p.player_name LIKE '%Dhoni%' AND m.season = 2018;

Q: Who won the orange cap in 2015?
A: SELECT p.player_name, SUM(b.batter_runs) AS total_runs
   FROM ball_by_ball b
   JOIN players p ON b.batter = p.player_name
   JOIN match_data m ON b.match_id = m.match_id
   WHERE m.season = 2015
   GROUP BY p.player_name
   ORDER BY total_runs DESC
   LIMIT 1;   

Q: What is the highest batting strike rate in IPL 2020 (min 100 balls)?
A: WITH PlayerStats AS (
       SELECT p.player_name,
              SUM(b.batter_runs) AS total_runs,
              COUNT(CASE WHEN b.is_wide_ball = 0 THEN 1 END) AS balls_faced
       FROM ball_by_ball b
       JOIN match_data m ON b.match_id = m.match_id
       JOIN players p ON b.batter = p.player_name
       WHERE m.season = 2020
       GROUP BY p.player_name
       HAVING balls_faced >= 100
   )
   SELECT player_name, total_runs, balls_faced,
          ROUND((total_runs * 100.0) / balls_faced, 2) AS strike_rate
   FROM PlayerStats
   ORDER BY strike_rate DESC
   LIMIT 1;

Q: Best batting average in IPL 2021 (min 200 runs)?
A: WITH BatterStats AS (
    SELECT b.batter, 
           SUM(b.batter_runs) AS total_runs,
           COUNT(CASE WHEN b.player_out = b.batter THEN 1 END) AS dismissals
    FROM ball_by_ball b
    JOIN match_data m ON b.match_id = m.match_id
    WHERE m.season = 2021
    GROUP BY b.batter
    HAVING total_runs >= 200 AND dismissals > 0
)
SELECT p.player_name, total_runs, dismissals,
       ROUND(CAST(total_runs AS FLOAT) / dismissals, 2) AS batting_average
FROM BatterStats bs
JOIN players p ON bs.batter = p.player_name
ORDER BY batting_average DESC
LIMIT 1;

Q: What is the best bowling average in IPL 2020 (min 10 wickets)?
A: WITH BowlingStats AS (
       SELECT p.player_name,
              COUNT(*) FILTER (WHERE b.is_wide_ball = 0 AND b.is_no_ball = 0) AS balls_bowled,
              SUM(b.total_runs) AS runs_conceded,
              COUNT(*) FILTER (WHERE b.is_wicket = 1 AND b.wicket_kind NOT IN ('run out', 'retired hurt')) AS wickets
       FROM ball_by_ball b
       JOIN players p ON b.bowler = p.player_name
       JOIN match_data m ON b.match_id = m.match_id
       WHERE m.season = 2020
       GROUP BY p.player_name
       HAVING wickets >= 10
   )
   SELECT player_name, runs_conceded, wickets,
          ROUND(CAST(runs_conceded AS FLOAT) / wickets, 2) AS bowling_average
   FROM BowlingStats
   ORDER BY bowling_average ASC
   LIMIT 1;

Q: Best bowling strike rate in IPL 2022 (min 10 wickets)?
A: WITH BowlerStats AS (
    SELECT b.bowler, COUNT(*) AS balls_bowled,
           COUNT(CASE WHEN b.is_wicket = 1 AND b.wicket_kind NOT IN ('run out', 'retired hurt', 'obstructing the field') THEN 1 END) AS wickets
    FROM ball_by_ball b
    JOIN match_data m ON b.match_id = m.match_id
    WHERE b.is_wide_ball = 0 AND b.is_no_ball = 0
      AND m.season = 2022
    GROUP BY b.bowler
    HAVING wickets >= 10
)
SELECT p.player_name, balls_bowled, wickets,
       ROUND(CAST(balls_bowled AS FLOAT) / wickets, 2) AS bowling_strike_rate
FROM BowlerStats bs
JOIN players p ON bs.bowler = p.player_name
ORDER BY bowling_strike_rate ASC
LIMIT 1;

Q: Most wickets in IPL history?
A: SELECT p.player_name, COUNT(*) AS total_wickets
FROM ball_by_ball b
JOIN players p ON b.player_out = p.player_name
WHERE b.is_wicket = 1
  AND b.wicket_kind NOT IN ('run out', 'retired hurt', 'obstructing the field')
GROUP BY p.player_name
ORDER BY total_wickets DESC
LIMIT 1;

Q:Who has the most Player of the Match awards in IPL history?
A: SELECT p.player_full_name, COUNT(*) AS motm_awards
FROM match_data m
JOIN players p ON m.player_of_match = p.player_id
GROUP BY p.player_full_name
ORDER BY motm_awards DESC
LIMIT 1;

Q: Who won the most Player of the Match awards in 2019?
A: SELECT p.player_full_name, COUNT(*) AS motm_awards
FROM match_data m
JOIN players p ON m.player_of_match = p.player_id
WHERE m.season = 2019
GROUP BY p.player_full_name
ORDER BY motm_awards DESC
LIMIT 1;

Q:Matches played at Wankhede in IPL history?

A:SELECT COUNT(DISTINCT match_id)
FROM match_data
WHERE venue = 'Wankhede Stadium';

Q:Matches at Eden Gardens in IPL 2024?

A:SELECT COUNT(DISTINCT match_id)
FROM match_data
WHERE venue = 'Eden Gardens'
  AND season = 2024;

Q:Matches won by batting first at Wankhede?

A:SELECT COUNT(DISTINCT match_id)
FROM match_data
WHERE venue = 'Wankhede Stadium'
  AND win_by_runs > 0;  

Now generate only the correct SQL query for this question:

Question: {question}
Return only the SQL query, nothing else.
"""

    model = genai.GenerativeModel("models/gemini-1.5-flash-latest")
    response = model.generate_content(prompt)
    logging.info(f"QUESTION: {question}")
    logging.info(f"SQL QUERY: {response.text.strip().split('```sql')[-1].split('```')[-2].strip() if '```sql' in response.text else response.text.strip()}")
    return response.text.strip().split("```sql")[-1].split("```")[-2].strip() if "```sql" in response.text else response.text.strip()

def execute_sql_query(sql_query):
    try:
        result = pd.read_sql(sql_query, conn)
        logging.info(f"SQL RESULT: {result.shape[0]} rows")
        return result
    except Exception as e:
        logging.error(f"SQL ERROR: {str(e)}")
        return str(e)

st.title("🏏 IPL Stats Chatbot")
st.write("Ask me anything about IPL stats (e.g., How many runs did Dhoni score in 2018?)")

question = st.text_input("Ask a question:")

if question:
    with st.spinner("Thinking..."):
        # Step 1: Get SQL query from Gemini
        sql_query = ask_gemini_for_sql(question)

        # Step 2: Execute SQL query
        result = execute_sql_query(sql_query)

        # Step 3: Convert results to readable text
        if isinstance(result, pd.DataFrame):
            if result.empty:
                st.warning("No results found for your question.")
            elif result.shape == (1, 1):
                # Single value, e.g., total runs = 973
                value = result.iloc[0, 0]
                col_name = result.columns[0].replace("_", " ").capitalize()
                st.success(f"**{col_name}: {value}**")
            elif result.shape[1] == 1:
                # One column multiple rows
                col = result.columns[0]
                values = result[col].tolist()
                st.success(f"**{col.replace('_', ' ').capitalize()}:**\n\n" + "\n".join(f"- {v}" for v in values))
            else:
                # Multiple columns - summarise each row
                summaries = []
                for _, row in result.iterrows():
                    summary = ", ".join(f"{col.replace('_', ' ')}: {val}" for col, val in row.items())
                    summaries.append(f"- {summary}")
                st.success("**Here are the results:**\n\n" + "\n".join(summaries))
        else:
            # If an error string is returned
            st.error(f"❌ Error: {result}")
