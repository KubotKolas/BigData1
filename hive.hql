USE default;

-- drop existing tables if they exist
DROP TABLE IF EXISTS mr_output_external;
DROP TABLE IF EXISTS teams_csv_external;
DROP TABLE IF EXISTS mr_output;
DROP TABLE IF EXISTS teams;
DROP TABLE IF EXISTS final_league_summary_json;

ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core-3.1.3.jar;

-- import MR output as external table
CREATE EXTERNAL TABLE mr_output_external (
    team_id STRING,             
    season STRING,              
    matches_played INT,         
    avg_goals_per_match DOUBLE  
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:mr_output_location}';


-- import CSV data as external table
-- CSV data description: team_id, name, city, league, coach
CREATE EXTERNAL TABLE teams_csv_external (
    team_id STRING,             
    name STRING,
    city STRING,
    league STRING,              
    coach STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    -- may need to replace ' with "
    "quoteChar" = '\"',
    "escapeChar" = "\\",
    "skip.header.line.count" = "1"
)
LOCATION '${hiveconf:teams_csv_location}';



-- create the final output table directly with JSON SerDe
CREATE EXTERNAL TABLE final_league_summary_json (
    league STRING,                   
    total_matches INT,               
    avg_goals_per_match DOUBLE,      
    teams_ranking ARRAY<STRUCT<team_id: STRING, rank_in_league: INT>>        
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES ("serialization.null.format"="null")
LOCATION '${hiveconf:json_output_location}';


-- populate the final_league_summary_json table
-- accessing data from external tables as CTEs/temp views

WITH mr_output AS (SELECT
    team_id,
    season,
    matches_played,            
    avg_goals_per_match,       
    CAST(matches_played * avg_goals_per_match AS DOUBLE) AS total_goals_scored_for_team_season
FROM mr_output_external),
teams AS (SELECT * FROM teams_csv_external)
INSERT OVERWRITE TABLE final_league_summary_json
SELECT
    league_aggs.league,
    league_aggs.total_matches,
    league_aggs.avg_goals_per_match,
    ranked_teams.teams_ranking_array AS teams_ranking
FROM (
    -- calculate league-level aggregates: total_matches, avg_goals_per_match
    SELECT
        t.league,
        -- divide by 2 to account for symmetry
        SUM(m.matches_played)/2 AS total_matches,
        -- divide by 2 to account for symmetry
        SUM(m.total_goals_scored_for_team_season) / (SUM(m.matches_played)/2) AS avg_goals_per_match
    FROM mr_output m
    JOIN teams t ON m.team_id = t.team_id
    GROUP BY t.league
) league_aggs
JOIN (
    -- calculate team rankings and prepare the array of structs
    SELECT
        league, 
        COLLECT_LIST(named_struct('team_id', team_id, 'rank_in_league', team_rank)) AS teams_ranking_array
    FROM (
        SELECT
            m.team_id,
            t.league,
            m.matches_played,
            ROW_NUMBER() OVER (PARTITION BY t.league ORDER BY m.matches_played DESC) AS team_rank
        FROM mr_output m
        JOIN teams t ON m.team_id = t.team_id
    ) ranked_teams_base
    GROUP BY league
) ranked_teams ON league_aggs.league = ranked_teams.league;