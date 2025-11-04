USE default;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS mr_output_external;
DROP TABLE IF EXISTS teams_csv_external;
DROP TABLE IF EXISTS mr_output_orc;
DROP TABLE IF EXISTS teams_orc;
DROP TABLE IF EXISTS final_league_summary_json; -- Renamed for clarity as the final output



-- 1. Import MR output as external table
CREATE EXTERNAL TABLE mr_output_external (
    team_id STRING,             
    season STRING,              
    matches_played INT,         
    avg_goals_per_match DOUBLE  
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:mr_output_location}';


-- 2. Import CSV data as external table
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
    "quoteChar" = "\"",
    "escapeChar" = "\\",
    "skip.header.line.count" = "1"
)
LOCATION '${hiveconf:teams_csv_location}';


-- 3. Convert MR output to ORC
CREATE TABLE mr_output_orc
STORED AS ORC
AS SELECT
    team_id,
    season,
    matches_played,            
    avg_goals_per_match,       
    CAST(matches_played * avg_goals_per_match AS DOUBLE) AS total_goals_scored_for_team_season
FROM mr_output_external;


-- 4. Convert CSV data to ORC
CREATE TABLE teams_orc
STORED AS ORC
AS SELECT * FROM teams_csv_external;


-- 5. Create the final output table directly with JSON SerDe
CREATE EXTERNAL TABLE final_league_summary_json (
    league STRING,                   
    total_matches INT,               
    avg_goals_per_match DOUBLE,      
    teams_ranking STRING             
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES ("serialization.null.format"="null")
LOCATION '${hiveconf:json_output_location}';


-- 6. Populate the final_league_summary_json table
INSERT OVERWRITE TABLE final_league_summary_json
SELECT
    league_aggs.league,
    league_aggs.total_matches,
    league_aggs.avg_goals_per_match,
    -- !!! Extremely complex and error-prone string manipulation for JSON !!!
    CONCAT(
        '[',
        CONCAT_WS(
            ',',
            COLLECT_LIST(
                CONCAT(
                    '{"team_id":"', ranked_teams_nested.team_id,
                    '", "rank_in_league":', CAST(ranked_teams_nested.rank_in_league AS STRING),
                    '}'
                )
            )
        ),
        ']'
    ) AS teams_ranking
FROM (
    -- Calculate league-level aggregates: total_matches, avg_goals_per_match
    SELECT
        t.league,
        SUM(m.matches_played) AS total_matches,
        SUM(m.total_goals_scored_for_team_season) / SUM(m.matches_played) AS avg_goals_per_match
    FROM mr_output_orc m
    JOIN teams_orc t ON m.team_id = t.team_id
    GROUP BY t.league
) league_aggs
JOIN (
    -- Calculate team rankings and prepare the array of structs (but don't collect yet)
    SELECT
        t.league,
        m.team_id,
        ROW_NUMBER() OVER (PARTITION BY t.league ORDER BY m.matches_played DESC) AS rank_in_league
    FROM mr_output_orc m
    JOIN teams_orc t ON m.team_id = t.team_id
) ranked_teams_nested ON league_aggs.league = ranked_teams_nested.league
GROUP BY league_aggs.league, league_aggs.total_matches, league_aggs.avg_goals_per_match -- Grouping here to re-collect the list for string manipulation
;