# NCAA Football - Match Preparation !

> In this project, I dive into the world of College Footbal, one of my favorite sports and league! This time, I'll analyze a bit of the big picture of the NCAA - as per the dataset - and then I'll prepare an analysis focused on a single team. The idea is imagining I'm working with a team and preparing to analyze our next opponent in order to put our team in the best position possible to win. Stick around to find out more! LET'S GO !!!!


 <br>
  <br>

## Table of Contents

- Business Questions
- Data Gathering
- Data Cleaning
- Exploratory Analysis
- Team Analysis and Game Preparation
- Presenting The Final Answers

 <br>
 <br>
 
## Business Questions

> As mentioned above, I will be preparing my team for an upcoming match against an NCAA team (It's still secret, gotta keep reading!). Coaches and staff came to me and asked me the following questions:

<details><summary> General NCAA Questions: </summary>
<p>

1. Which **Offense** scored the most Touchdowns and what season was it?
2. Which **Defense** allowed the least Touchdowns and in what season?
3. In a season, which team had the most **Passing** Touchdowns? And **Rushing**?
4. Which Team has scored the most **Field Goals**, between the 2013 and 2022 Seasons?
5. What’s the **average punt return yardage allowed** per game - by division - for the first 5 seasons (2013-2017) and how does it compare against the last 5 seasons (2018-2022?
6. What’s the average difference between scored and allowed TD’s ? Does a bigger difference result in more wins?
7. Which team had the highest a**verage of time of possession** in each season?
8. Which Defense lead the league in **Interceptions** and **Fumbles recovered** in each season?

</p>
</details>

<br>
<br>

<details><summary> Game Preparation Questions: </summary>
<p>

1. How many wins did our opponent had  each season, and the rolling average of wins?
2. How many Offensive Yards were they able to get? And how many yards has their defense allowed?
3. **Special Teams Analysis - Kickoff Returns:**
   - What’s their average yardage per kickoff return?
   - How many returns per game do they average?
   - What’s their average kickoff return yardage, per game?
4. **Offense Analysis:**
   - What’s the average offensive plays per game? And average Yards per Play?
   - How many 1st downs, on average, do Michigan play per game?
   - Do they prefer to run or pass, on first downs?
   - How’s their Redzone Offense?
   - What’s their average Time of Possession?
5. **Special Teams Analysis - Punting and Field Goals:**
   - Number of Punts
   - Punt Return TDs and Yds
   - Total Field Goal Attempts
6. **Defense Analysis:**
   - Yards allowed per play
   - Rush yards allowed vs Pass Yards allowed
   - Pass Completion % Allowed
   - 3rd Down Conversion allowed
   - Redzone Defense
   - Total sacks and yards per sack
   - Defensive points
   - Takeaways
7. **Special Teams Analysis:**
   - Total punt returns
   - Punt return yards and TDs
   - Opponent field goals made, redzone


</p>
</details>

 <br>
 <br>
  
## Data Gathering

> To answer all these questions, I used [this dataset here!](https://www.kaggle.com/datasets/jeffgallini/college-football-team-stats-2019)

Let's understand how this dataset is organized. <br>

We can find 10 .csv files, one for each season, starting on the 2013 season and ending in 2022. These files contain data collected for all of the FBS level teams. Includes stats for: <bre>

- Offense
- Defense
- Turnovers
- Redzone
- Special Teams
- First, Third and Fourth Downs
- and more!

> Analyzing the data structure we find:<br>

 - **cfb13**, **cfb14**, **cfb15** have <ins>146</ins> columns.
 - **cfb16**, **cfb17**, **cfb18**, **cfb19**, **cfb20**, **cfb21** have <ins>152</ins> columns.
 - **cfb22** has <ins>151</ins> columns.

> All the files were uploaded into BigQuery, where I'll continue with the project.



<br>
<br>


## Data Cleaning

> In order to better organize our data, I will create FOUR final tables (and add Primary Keys / Foreign Keys).
> I will guide you through this process and sharing my notes and findings along the way

1. **teams_divisions**: table containing the names and divisions of each team through every season (13-22)
   - Some issues with data input. Some teams or divisions are spelled differently and needed to be consistent.
   - “Team” field showed the college name and division between brackets (Ex: Penn St. (Big Ten). So, I decided to split these into 2 columns (Team and Division). This allows for a clearer view of whether or not the team changed division throughout the years.
   - In some seasons, we may find teams without a division associated with it, or wrong division (“Ole Miss()” or “Pittsburgh ()” and “New Mexico St. (FBS Independent)” or “New Mexico St. (Independent)” ). Here, I will complete this information with the corresponding division for that season.
<details><summary>SQL Code: </summary>
<p>

``` SQL
CREATE OR REPLACE TABLE ncaa_fbs_stats.teams_divisions AS(

SELECT -- 2013 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2013"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2013"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2013') 
  END AS Id,                                                -- Using CONCAT to create an unique identifier for each team
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1) 
  END AS Team,                                               -- SUBSTR / STRPOS to extract just the Team name / college
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,                                              -- SUBSTR / STRPOS / LENGTH to extract just the Division
  2013 AS Season                                                -- Added column for a Season identifier

FROM subtle-striker-370721.ncaa_fbs_stats.cfb13

UNION ALL

SELECT -- 2014 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2014"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2014"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2014') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN 
      Team LIKE "Pittsburgh%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "ACC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2014 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb14
UNION ALL

SELECT -- 2015 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2015"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2015"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2015') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2015 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb15

UNION ALL

SELECT -- 2016 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2016"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2016"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2016') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2016 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb16

UNION ALL

SELECT -- 2017 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2017"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2017"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2017') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
    WHEN -- "New Mexico St. (Independent)"
      Team LIKE "%(Independent)%"
        THEN "FBS Independent"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2017 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb17

UNION ALL

SELECT -- 2018 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2018"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2018"
    WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina2018"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2018') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2018 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb18

UNION ALL

SELECT -- 2019 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2019"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2019"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2019') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2019 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb19

UNION ALL

SELECT -- 2020 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2020"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2020"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2020"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2020') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2020 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb20

UNION ALL

SELECT -- 2021 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2021"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2021"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2021"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2021') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2021 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb21

UNION ALL

SELECT -- 2022 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2022"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2022"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2022"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2022') 
  END AS Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN NULL
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2022 AS Season

FROM subtle-striker-370721.ncaa_fbs_stats.cfb22
)
```
</p>
</details>
     
2. **offense**: table containing offensive statistics for each team and through every season (13-22)
   - Same issues as above, corrected for consistency.
   - Not pulling the calculated columns (Ex: off_yards_play) because I may want to do these calculations later.
   - X4th_Down_Attempts and X4th_Down_Conversions entries are switched. For example it would say a team had 8 attempts and converted 22, which is incorrect. Needs to be changed.
   - Casting Time_of_Possession as TIME to better help analysis.
   - For the  2021 and 2022 season’s tables, there was a Win/Loss column instead of a Win Column and a Loss Column. Had to split these to align with the other tables.
    
<details><summary>SQL Code: </summary>
<p>
  
``` SQL
CREATE OR REPLACE TABLE ncaa_fbs_stats.offense AS(

SELECT -- 2013 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2013"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2013"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2013') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2013 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb13

UNION ALL

SELECT -- 2014 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2014"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2014"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2014') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN 
      Team LIKE "Pittsburgh%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "ACC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2014 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb14

UNION ALL

SELECT -- 2015 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2015"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2015"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2015') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2015 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb15

UNION ALL

SELECT -- 2016 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2016"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2016"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2016') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2016 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb16

UNION ALL

SELECT -- 2017 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2017"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2017"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2017') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN 
      Team LIKE "%(Independent)%"
        THEN "FBS Independent"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2017 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb17

UNION ALL

SELECT -- 2018 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2018"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2018"
        WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina2018"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2018') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2018 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb18

UNION ALL

SELECT -- 2019 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2019"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2019"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2019') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2019 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb19

UNION ALL

SELECT -- 2020 Season
   CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2020"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2020"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2020"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2020') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2020 AS Season,
  Games,
  Win, 
  Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  X3rd_Attempts AS X3rd_Down_Attempts,
  X3rd_Conversions AS X3rd_Down_Conversions,
  X4th_Conversions AS X4th_Down_Attempts, 
  X4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb20

UNION ALL

SELECT -- 2021 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2021"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2021"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2021"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2021') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2021 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss,
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  _3rd_Attempts AS X3rd_Down_Attempts,
  _3rd_Conversions AS X3rd_Down_Conversions,
  _4th_Conversions AS X4th_Down_Attempts, 
  _4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  _2_Point_Conversions AS X2_Point_Conversions,
  PARSE_TIME("%M:%S", REPLACE(CAST(Average_Time_of_Possession_per_Game AS STRING FORMAT "00.00"), ".", ":")) AS Average_Time_of_Possession_per_Game,
  
FROM subtle-striker-370721.ncaa_fbs_stats.cfb21

UNION ALL

SELECT -- 2022 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2022"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2022"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2022"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2022') 
  END AS off_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2022 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss, 
  Off_Plays,
  Off_Yards,
  Off_TDs,
  First_Down_Runs,
  First_Down_Passes,
  First_Down_Penalties,
  First_Downs,
  _3rd_Attempts AS X3rd_Down_Attempts,
  _3rd_Conversions AS X3rd_Down_Conversions,
  _4th_Conversions AS X4th_Down_Attempts, 
  _4th_Attempts AS X4th_Down_Conversions,
  Rush_Attempts,
  Rush_Yds AS Rush_Yards,
  Rushing_TD,
  Fumbles_Lost,
  Pass_Attempts,
  Pass_Completions,
  Interceptions_Thrown_x,
  Interceptions_Thrown_y,
  Pass_Yards,
  Pass_Touchdowns AS Pass_TD,
  Redzone_Attempts,
  Redzone_Rush_TD,
  Redzone_Pass_TD,
  _2_Point_Conversions AS X2_Point_Conversions,
  PARSE_TIME("%M:%S", Average_Time_of_Possession_per_Game) AS Average_Time_of_Possession_per_Game

FROM subtle-striker-370721.ncaa_fbs_stats.cfb22

)

```
</p>
</details>

3. **defense**: table containing defensive statistics for each team and through every season (13-22)
   - Same issues as above, corrected for consistency
  
<details><summary>SQL Code: </summary>
<p>
  
``` SQL
CREATE OR REPLACE TABLE ncaa_fbs_stats.defense AS(
SELECT -- 2013 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2013"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2013"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2013') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2013 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb13

UNION ALL

SELECT -- 2014 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2014"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2014"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2014') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN 
      Team LIKE "Pittsburgh%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "ACC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2014 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb14

UNION ALL

SELECT -- 2015 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2015"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2015"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2015') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2015 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb15

UNION ALL

SELECT -- 2016 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2016"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2016"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2016') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
        THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2016 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb16

UNION ALL

SELECT -- 2017 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2017"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2017"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2017') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
        THEN "SEC"
    WHEN 
      Team LIKE "%(Independent)%"
        THEN "FBS Independent"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2017 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb17

UNION ALL

SELECT -- 2018 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2018"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2018"
        WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina2018"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2018') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2018 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb18

UNION ALL

SELECT -- 2019 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2019"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2019"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2019') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2019 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,


FROM subtle-striker-370721.ncaa_fbs_stats.cfb19

UNION ALL

SELECT -- 2020 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2020"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2020"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2020"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2020') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2020 AS Season,
  Games,
  Win, 
  Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb20

UNION ALL

SELECT -- 2021 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2021"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2021"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2021"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2021') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2021 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss, 
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb21

UNION ALL

SELECT -- 2022 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2022"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2022"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2022"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2022') 
  END AS def_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2022 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss,  
  Def_Plays,
  Yards_Allowed,
  Off_TDs_Allowed,
  Opp_First_Down_Runs,
  Opp_First_Down_Passes,
  Opp_First_Down_Penalties
  Opp_First_Downs,
  Opp_3rd_Attempt,
  Opp_3rd_Conversion,
  Opp_4th_Attempt,
  Opp_4th_Conversion,
  Opp_Pass_Attempts,
  Opp_Completions_Allowed,
  Opp_Pass_Yds_Allowed,
  Opp_Pass_TDs_Allowed,
  Opp_Rush_Attempts,
  Opp_Rush_Yards_Alloweed AS Opp_Rush_Yds_Allowed,
  Opp_Rush_Touchdowns_Allowed AS Opp_Rush_TDs_Allowed,
  Opp_Redzone_Attempts,
  Opp_Redzone_Rush_TD_Allowed,
  Opp_Redzone_Pass_Touchdowns_Allowed,
  Sacks,
  Sack_Yards,
  Opp_Safety,
  Defensive_Points,
  Total_Tackle_For_Loss,
  Tackle_For_Loss_Yards,
  Opponents_Intercepted,
  Fumbles_Recovered,

FROM subtle-striker-370721.ncaa_fbs_stats.cfb22

)

```
</p>
</details>

4. **special_teams**: table containing special teams statistics for each team and through every season (13-22)
   - Same issues as the previous tables, corrected for consistency.
   - Column Avg_Yard_per_Kickoff_Return is wrong. This column tracks the total kickoff returns for touchdown. Kickoff_Return_Touchdowns values are showing the total yardage for kickoff returns. Kickoff_Return_Yards is measuring the total kickoff touchbacks, will be deleted as we already have a column for that measure (Kickoff_Touchbacks). This behaviour is showing from the 2016 season to the 2021. Will be corrected.
   - Renaming some columns for clearer understanding

<details><summary>SQL Code: </summary>
<p>

``` SQL
CREATE OR REPLACE TABLE ncaa_fbs_stats.special_teams AS(

SELECT -- 2013 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2013"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2013"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2013') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2013 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Yards AS Kickoff_Return_Yds,
  Kickoff_Return_Touchdowns AS Kickoff_Return_TDs,
  NULL AS Opp_Kickoff_Return,
  NULL AS Kickoff_Touchback,
  NULL AS Opp_Kickoff_Return_Yds,
  NULL AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb13


UNION ALL

SELECT -- 2014 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2014"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2014"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2014') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN 
      Team LIKE "Pittsburgh%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "ACC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2014 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Yards AS Kickoff_Return_Yds,
  Kickoff_Return_Touchdowns AS Kickoff_Return_TDs,
  NULL AS Opp_Kickoff_Return,
  NULL AS Kickoff_Touchback,
  NULL AS Opp_Kickoff_Return_Yds,
  NULL AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb14


UNION ALL

SELECT -- 2015 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2015"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2015"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2015') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2015 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Yards AS Kickoff_Return_Yds,
  Kickoff_Return_Touchdowns AS Kickoff_Return_TDs,
  NULL AS Opp_Kickoff_Return,
  NULL AS Kickoff_Touchback,
  NULL AS Opp_Kickoff_Return_Yds,
  NULL AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb15

  
UNION ALL

SELECT -- 2016 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2016"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2016"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2016') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2016 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb16

  
UNION ALL

SELECT -- 2017 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2017"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2017"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2017') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
    WHEN -- "New Mexico St. (Independent)"
      Team LIKE "%(Independent)%"
        THEN "FBS Independent"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2017 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb17

    
UNION ALL

SELECT -- 2018 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2018"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2018"
        WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina2018"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2018') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN
      Team LIKE "Coastal%"
        THEN "Coastal Carolina"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2018 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb18

  
UNION ALL

SELECT -- 2019 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2019"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2019"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2019') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2019 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb19

    
UNION ALL

SELECT -- 2020 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2020"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2020"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2020"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2020') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2020 AS Season,
  Games,
  Win, 
  Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb20

    
UNION ALL

SELECT -- 2021 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2021"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2021"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2021"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2021') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2021 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Touchdowns AS Kickoff_Return_Yds,
  Avg_Yard_per_Kickoff_Return AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Feild_Goals AS FG_Made,
  Opp_Feild_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb21

    
UNION ALL

SELECT -- 2022 Season
  CASE
    WHEN
      Team LIKE "Miami (OH)"
        THEN "Miami (OH)2022"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)2022"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St.2022"
    ELSE 
      CONCAT(SUBSTR(Team, 0, STRPOS(Team, ' (') -1),'2022') 
  END AS spt_Id,
  CASE
    WHEN
      Team LIKE "Miami (OH)%"
        THEN "Miami (OH)"
    WHEN
      Team LIKE "Miami (FL)%"
        THEN "Miami (FL)"
    WHEN 
      Team LIKE "App State%"
        THEN "Appalachian St."
    ELSE
      SUBSTR(Team, 0, STRPOS(Team, ' (') -1)
  END AS Team,
  CASE
    WHEN 
      Team LIKE "%(MWC)%"
        THEN "Mountain West"
    WHEN 
      Team LIKE "Miami%"
        THEN
          SUBSTR(Team, STRPOS(Team,') (') + 3,LENGTH(Team) - (STRPOS(Team, ') (') + 3))
    WHEN 
      Team LIKE "Ole Miss%" AND LENGTH(SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1))) = 0
      THEN "SEC"
      ELSE 
        SUBSTR(Team,STRPOS(Team,'(') + 1,LENGTH(Team) - (STRPOS(Team,'(')+1)) 
  END AS Division,
  2022 AS Season,
  Games,
  CAST(SUBSTR(Win_Loss, 0, STRPOS(Win_Loss, '-') -1) AS INT64) AS Win, 
  CAST(SUBSTR(Win_Loss, STRPOS(Win_Loss, '-') +1) AS INT64) AS Loss,
  Kickoffs_Returned AS Kickoff_Return,
  Kickoff_Return_Yards AS Kickoff_Return_Yds,
  Kickoff_Return_Touchdowns AS Kickoff_Return_TDs,
  Opp_Kickoff_Returns AS Opp_Kickoff_Return,
  Kickoff_Touchbacks AS Kickoff_Touchback,
  Opponent_Kickoff_Return_Yards AS Opp_Kickoff_Return_Yds,
  Opp_Kickoff_Return_Touchdowns_Allowed AS Opp_Kickoff_Return_TDs,
  Punt_Returns AS Punt_Return,
  NET_Punt_Return_Yards AS Punt_Return_Yds,
  Punt_Return_Touchdowns AS Punt_Return_TDs,
  Opp_Punt_Returns AS Opp_Punt_Return,
  Opp_NET_Punt_Return_Yards AS Opp_Punt_Return_Yds,
  Opp_Punt_Return_Touchdowns_Allowed AS Opp_Punt_Return_TDs,
  Redzone_Field_Goals_Made AS Redzone_FG_Made,
  Opp_Redzone_Field_Goals_Made AS Opp_Redzone_FG_Made,
  PAT AS PAT_Made,
  Opponent_Extra_Points AS Opp_PAT_Made,
  Opp_Deflected_Extra_Points AS Opp_Deflected_Extra_Point,
  Field_Goals AS FG_Made,
  Opp_Field_Goals_Made AS Opp_FG_Made,


FROM 
  subtle-striker-370721.ncaa_fbs_stats.cfb22

)
```
</p>
</details>
  
> Tables were merged, previously detected issues are cleaned and ready for analysis!

 
## Exploratory Analysis
### 1. Which **Offense** scored the most Touchdowns and what season was it?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 2. Which **Defense** allowed the least Touchdowns and in what season?
> <details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:


### 3. In a season, which team had the most **Passing** Touchdowns? And **Rushing**?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 4. Which Team has scored the most **Field Goals**, between the 2013 and 2022 Seasons?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 5. What’s the **average punt return yardage allowed** per game - by division - for the first 5 seasons (2013-2017) and how does it compare against the last 5 seasons (2018-2022?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 6. What’s the average difference between scored and allowed TD’s ? Does a bigger difference result in more wins?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 7. Which team had the highest a**verage of time of possession** in each season?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:



### 8. Which Defense lead the league in **Interceptions** and **Fumbles recovered** in each season?
<details><summary>SQL Code: </summary>
<p>

``` SQL

```
</p>
</details>

> Query 1 results:













 <br>
 <br>
 <br>
  
## Presenting The Final Answers
 
<details><summary> Questions 1, 2 and 3: </summary>
<p>

1. Which Pokémon has the highest total points? And the lowest? <br>
>Q1: Gen VII Wishiwashi is the lowest total scoring Pokémon, making him the weakest today.  <br>
 Eternatus Eternamax, a mysterious alien species is the strongest with an absurd total of 1125! It was first introduced in Gen VIII <br><br>
 
2. How many types are there and how are Pokémon distributed through them? <br>
>Q2: There are 18 unique Pokémon Types. 176 Pokémon are Water-type, the most common. Only 65 are Ice-type, making it these the hardest to find. Several Pokémon have 2 types!<br><br>
 
3. How many Generations are there today and how many Pokémon do each Generation have? <br>
>Q3: We can now see how our current 1008 Pokémon are distributed through the 9 Generations. Do you still remember the original 151?<br>
 The first Generation introduced the second most Pokémon. Gen V topped that with 156 new species!<br><br>
 
![image](https://user-images.githubusercontent.com/11091531/220209977-6749b226-cfee-4a3d-948d-e6c846de9d85.png)


</p>
</details> <br><br>
 
 <details><summary> Questions 4: </summary>
<p>

4. How do Legendary Pokémon compare to Normal Pokémon? <br>
>Q4: Here we can find that Legendary Pokémon are divided into 3 classes (or sub-categories): Legendary, Sub-Legendary, Mythical. <br>
 Pokémon that are not part of any of these classes, belong to the Normal class. <br>
 How many Pokémon does each Legendary class have? Are they stronger than Normal class Pokémon?<br>
 
 >We can see in our pie chart that we have only 22 Mythical Pokémon, the rarest kind. Sub-Legendary are over 50 different species and Legendary, solid 26. <br>
 Without surprise, Legendary Pókemon are the strongest type! There isn't much of a difference between Mythical and Sub-Legendary but the first class has an advantage.<br> The Normal class is clearly weaker in average. The gap in between Normal and Legendary is almost 200 Total Points!
 
![image](https://user-images.githubusercontent.com/11091531/220212709-2798a543-0768-4c24-a08e-8c679c69587a.png)


</p>
</details> <br><br>
 
 
 <details><summary> Questions 5: </summary>
<p>

5. Which Pokémon from Gen I and Ground type are more "petable"? <br>
>Q5: Considering we found out that there are Pokémon who are not very friendly - 0 base friendship! - I'm happy that the Gen I Ground-type Pokémon are all striking 60, which is a little bit above the average (55,04). <br>
 That being said, I don't think I'd try and pet a Rhyhorn... Maybe a Cubone, but hoping there's not a Marowak near by! <br>
 Would you pet any of these Pokémon?
 
 
![image](https://user-images.githubusercontent.com/11091531/220214123-af06c48a-b265-4bde-b9b0-3626e16649fb.png)


</p>
</details> <br><br>
 
  <details><summary> Questions 6: </summary>
<p>

6. Which Pokémon are Purple in all their Evolutionary Stages and are Air or Water type? <br>
>Q6: Even though this was a challenging query, the results aren't big. In fact, we can see that only 1 Pokémon is Purple in all it's Evolutionary Stages, while also being Water-type or Air-Type. The Zubat Family. <br>
(I still have nightmares with the Zubat Cave...) <br>
 
 
![image](https://user-images.githubusercontent.com/11091531/220214306-5083146f-8401-4562-a546-f870a6bdd223.png)


</p>
</details> <br><br>
 
 
 <details><summary> Questions 7: </summary>
<p>

7. Analysis of my Favorite Pokémon <br>
>Q6: Squirtle and Psyduck are my favorite Pokémon. They're both from Gen I, Water-Type and in their base form. <br>
 Psyduck, a Yellow Pokémon, can evolve once. It's final form is Golduck, a Blue colored Pokémon.
 Squirtle can evolve twice. It's second stage form is Wartotle and third stage is Blastoise. All 3 are Blue. <br><br>
 Analysing the results for Query #4, Squirtle and Psyduck are below average in Total Points against all the other groupings explored. In some stats, they surpass the average for the "Normal Class Base Stage Gen I" and "Normal Class Base Stage All Generations" (Defense and Special Defense, for Squirtle. Special Attack, for Psyduck) <Br>Once they get to their Final Stage, their Total Points strike well above the other groupings averages, but still not reaching the Lengendary Classes. It's actually pretty impressive how strong they become!<br>
 Picking up on Query #5, we can understand that both Squirtle and Psyduck stand on the low end of the box plots, below the median - not great. <br>
 But then, Golduck and Blastoise walk peacefully above the median of all grouping (except, of course, the 3 Legendary Classes). <br><br>
 How about you? Do you have any favorite Pokémon? Let me know! <br><br>
 
 
![image](https://user-images.githubusercontent.com/11091531/220216815-7b91353e-489d-441a-9811-a5f6651e2d49.png)


</p>
</details> <br><br>

## Conclusion
 
 Thank you for taking the time to explore this project. It's my first (published) project and even though it's a bit large I had an amazing time elaborating this. It's main purpose was to help practice SQL, Excel, Visualization, Presentation skills (the whole show) while having fun at the same time. I'd say it was a huge success! <br>
 Can't wait to get my hands on the next project. <br><br>
 I'll see you then!
