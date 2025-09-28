create database olympics;
use olympics;

create table if not exists olympics_history
(
    id varchar(255),
	name VARCHAR(255),
    sex VARCHAR(255),
    age VARCHAR(255),
    height VARCHAR(255),
    weight VARCHAR(255),
    team VARCHAR(255),
    noc VARCHAR(255),
    games VARCHAR(255),
    year varchar(255),
    season VARCHAR(255),
    city VARCHAR(255),
    sport VARCHAR(255),
    event VARCHAR(255),
    medal VARCHAR(255)
    );

create table if not exists olympics_history_noc_regions
(
    noc VARCHAR(50),
    region varchar(100),
    notes varchar(100)
);

LOAD DATA INFILE 'athlete_events.csv'
INTO TABLE olympics_history
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, name, sex, age, height, weight, team, noc, games, year, season, city, sport,event,medal);


LOAD DATA INFILE 'noc_regions.csv'
INTO TABLE olympics_history_noc_regions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(noc ,regions ,notes);

SELECT * FROM olympics_history;

SELECT COUNT(DISTINCT games) AS total_games
FROM olympics_history;

SELECT DISTINCT Games, Year, Season, City
FROM olympics_history
ORDER BY Year;

SELECT Games, COUNT(DISTINCT NOC) AS total_nations
FROM olympics_history
GROUP BY Games
ORDER BY Games;

SELECT Year, total_nations
FROM (
    SELECT Year, COUNT(DISTINCT NOC) AS total_nations
    FROM olympics_history
    GROUP BY Year
) AS nation_counts
WHERE total_nations = (SELECT MAX(total_nations) FROM (
                          SELECT Year, COUNT(DISTINCT NOC) AS total_nations
                          FROM olympics_history
                          GROUP BY Year
                       ) AS subquery)
   OR total_nations = (SELECT MIN(total_nations) FROM (
                          SELECT Year, COUNT(DISTINCT NOC) AS total_nations
                          FROM olympics_history
                          GROUP BY Year
                       ) AS subquery);

SELECT NOC
FROM olympics_history
GROUP BY NOC 
HAVING COUNT(DISTINCT Games)=(
SELECT COUNT(DISTINCT Games)
FROM olympics_history);

SELECT Sport
FROM olympics_history
 WHERE season = 'summer'
 GROUP BY Sport
 HAVING COUNT(DISTINCT Games)=(
 SELECT COUNT(DISTINCT Games)
 FROM olympics_history
 WHERE season = 'summer');
 
 SELECT Sport , MIN(Games) AS Played_In
 FROM olympics_history
 GROUP BY Sport
 HAVING COUNT(DISTINCT Games) = 1;
 
SELECT Games , COUNT(DISTINCT Sport) AS total_sport
FROM olympics_history
GROUP BY Games
ORDER BY Games;

SELECT id, name, sex, age, height, weight, team, noc, games, year, season, city, sport,event,medal
FROM olympics_history
WHERE Medal = 'Gold'
AND Age = (
SELECT MAX(Age)
      FROM olympics_history
      WHERE Medal = 'Gold' AND Age IS NOT NULL 
      );
 
 SELECT Name, Age, Team, Games, Sport, Event, Medal
FROM olympics_history
WHERE Medal = 'Gold' AND Age IS NOT NULL
ORDER BY Age DESC
LIMIT 1;


SELECT Name, Age, Team, Games, Sport, Event, Medal
FROM olympics_history
WHERE UPPER(TRIM(Medal)) = 'GOLD'
  AND Age <> 'NA'
ORDER BY CAST(Age AS UNSIGNED) DESC
LIMIT 1;


SELECT Name, Age, Team, Games, Sport, Event, Medal
FROM olympics_history
WHERE Medal LIKE '%Gold%'
ORDER BY Age DESC
LIMIT 1;

SELECT Name, Age, Team, Games, Sport, Event, Medal
FROM olympics_history
WHERE Medal LIKE '%Gold%'
  AND Age = (
      SELECT MAX(Age)
      FROM olympics_history
      WHERE Medal LIKE '%Gold%'
  );

SELECT sex,
COUNT(DISTINCT id) AS participants
FROM 	olympics_history
GROUP BY sex;

SELECT 
    Name,Medal,
    COUNT(Medal) AS Total_Medals
FROM olympics_history
WHERE Medal <> 'NA'
GROUP BY Name,Medal
ORDER BY Total_Medals DESC
LIMIT 5;

SELECT 
    Name, 
    COUNT(Medal) AS Total_Medals
FROM olympics_history
WHERE Medal <> 'NA'
GROUP BY Name
ORDER BY Total_Medals DESC
LIMIT 5;

SELECT 
    Name, 
    Medal, 
    Year, 
    Games, 
    Team, 
    Sport, 
    Event
FROM olympics_history
WHERE Medal IS NOT NULL AND Medal <> 'NA'
ORDER BY Name, Year;

SELECT 
   Team, 
    COUNT(Medal) AS Total_Medals
FROM olympics_history
WHERE Medal <> 'NA'
GROUP BY Team
ORDER BY Total_Medals DESC
LIMIT 5;


WITH MEDALCOUNT AS (
SELECT Team, COUNT(Medal) AS Total_Medals
FROM olympics_history
WHERE Medal <> ' NA'
GROUP BY Team
)
SELECT * FROM MEDALCOUNT
ORDER BY Total_Medals DESC
LIMIT 5;

WITH MedalCount AS (
    SELECT 
        Team,
        SUM(CASE WHEN Medal = 'Gold' THEN 1 ELSE 0 END) AS Gold_Medals,
        SUM(CASE WHEN Medal = 'Silver' THEN 1 ELSE 0 END) AS Silver_Medals,
        SUM(CASE WHEN Medal = 'Bronze' THEN 1 ELSE 0 END) AS Bronze_Medals,
        COUNT(Medal) AS Total_Medals
    FROM olympics_history
    WHERE Medal IS NOT NULL AND Medal <> 'NA'
    GROUP BY Team
)
SELECT *
FROM MedalCount
ORDER BY Total_Medals DESC;

WITH MedalCount AS (
    SELECT 
        Team,
        SUM(CASE WHEN UPPER(TRIM(Medal)) = 'GOLD' THEN 1 ELSE 0 END) AS Gold_Medals,
        SUM(CASE WHEN UPPER(TRIM(Medal)) = 'SILVER' THEN 1 ELSE 0 END) AS Silver_Medals,
        SUM(CASE WHEN UPPER(TRIM(Medal)) = 'BRONZE' THEN 1 ELSE 0 END) AS Bronze_Medals,
        COUNT(*) AS Total_Medals
    FROM olympics_history
    WHERE Medal IS NOT NULL AND UPPER(TRIM(Medal)) <> 'NA'
    GROUP BY Team
)
SELECT *
FROM MedalCount
ORDER BY Total_Medals DESC;

WITH MedalCount AS (
    SELECT
        Team,
        SUM(UPPER(Medal) LIKE '%GOLD%')   AS Gold_Medals,
        SUM(UPPER(Medal) LIKE '%SILVER%') AS Silver_Medals,
        SUM(UPPER(Medal) LIKE '%BRONZE%') AS Bronze_Medals,
        (
          SUM(UPPER(Medal) LIKE '%GOLD%') +
          SUM(UPPER(Medal) LIKE '%SILVER%') +
          SUM(UPPER(Medal) LIKE '%BRONZE%')
        ) AS Total_Medals
    FROM olympics_history
    GROUP BY Team
)
SELECT *
FROM MedalCount
ORDER BY Total_Medals DESC;

WITH MedalCount AS (
    SELECT
        Team,Games,
        SUM(UPPER(Medal) LIKE '%GOLD%')   AS Gold_Medals,
        SUM(UPPER(Medal) LIKE '%SILVER%') AS Silver_Medals,
        SUM(UPPER(Medal) LIKE '%BRONZE%') AS Bronze_Medals,
        (
          SUM(UPPER(Medal) LIKE '%GOLD%') +
          SUM(UPPER(Medal) LIKE '%SILVER%') +
          SUM(UPPER(Medal) LIKE '%BRONZE%')
        ) AS Total_Medals
    FROM olympics_history
    GROUP BY Team,Games
)
SELECT *
FROM MedalCount
ORDER BY Total_Medals DESC;



WITH MedalCount AS (
    SELECT
        Team,Games,
        SUM(UPPER(Medal) LIKE '%GOLD%')   AS Gold_Medals,
        SUM(UPPER(Medal) LIKE '%SILVER%') AS Silver_Medals,
        SUM(UPPER(Medal) LIKE '%BRONZE%') AS Bronze_Medals,
        (
          SUM(UPPER(Medal) LIKE '%GOLD%') +
          SUM(UPPER(Medal) LIKE '%SILVER%') +
          SUM(UPPER(Medal) LIKE '%BRONZE%')
        ) AS Total_Medals
    FROM olympics_history
    GROUP BY Team,Games
)
SELECT *
FROM MedalCount
ORDER BY Total_Medals DESC;



WITH MedalCount AS (
    SELECT
        Games,
        Team,
        SUM(CASE WHEN UPPER(Medal) LIKE '%GOLD%'   THEN 1 ELSE 0 END) AS Gold_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END) AS Silver_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END) AS Bronze_Medals
    FROM olympics_history
    WHERE Medal IS NOT NULL
    GROUP BY Games, Team
),
Winners AS (
    SELECT Games, Team, 'Gold'   AS MedalType, Gold_Medals   AS CountMedals FROM MedalCount
    UNION ALL
    SELECT Games, Team, 'Silver' AS MedalType, Silver_Medals FROM MedalCount
    UNION ALL
    SELECT Games, Team, 'Bronze' AS MedalType, Bronze_Medals FROM MedalCount
)
SELECT Games, MedalType, Team, CountMedals
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY Games, MedalType ORDER BY CountMedals DESC) AS rnk
    FROM Winners
) t
WHERE rnk = 1
ORDER BY Games, MedalType;




WITH MedalCount AS (
    SELECT
        Team,
        Games,
        SUM(CASE WHEN UPPER(Medal) LIKE '%GOLD%'   THEN 1 ELSE 0 END) AS Gold_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END) AS Silver_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END) AS Bronze_Medals,
        COUNT(Medal) AS Total_Medals
    FROM olympics_history
    GROUP BY Team, Games
),
Winners AS (
    SELECT Games, Team, 'Gold'   AS MedalType, Gold_Medals   AS CountMedals FROM MedalCount
    UNION ALL
    SELECT Games, Team, 'Silver' AS MedalType, Silver_Medals FROM MedalCount
    UNION ALL
    SELECT Games, Team, 'Bronze' AS MedalType, Bronze_Medals FROM MedalCount
    UNION ALL
    SELECT Games, Team, 'Total'  AS MedalType, Total_Medals  FROM MedalCount
),
Ranked AS (
    SELECT
        Games,
        MedalType,
        Team,
        CountMedals,
        RANK() OVER (PARTITION BY Games, MedalType ORDER BY CountMedals DESC) AS rnk
    FROM Winners
)
SELECT Games, MedalType, Team, CountMedals
FROM Ranked
WHERE rnk = 1
ORDER BY Games, MedalType;


WITH MedalCount AS (
SELECT 
      Team,
	SUM(CASE WHEN UPPER(Medal) LIKE '%GOLD%' THEN 1 ELSE 0 END) AS Gold_medals,
    SUM(CASE WHEN UPPER(Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END) AS Silver_medals,
    SUM(CASE WHEN UPPER(Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END) AS Bronze_medals
    FROM olympics_history
    GROUP BY Team
    )
    SELECT Gold_medals,Silver_medals,Bronze_medals 
    FROM MedalCount
    WHERE Gold_medals = 0
    AND(Silver_medals>0 OR Bronze_medals > 0)
    ORDER BY (Silver_medals + Bronze_medals) DESC;
    
  WITH MedalCount AS (
    SELECT
        Team,
        SUM(CASE WHEN UPPER(Medal) LIKE '%GOLD%'   THEN 1 ELSE 0 END)   AS Gold_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END)   AS Silver_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END)   AS Bronze_Medals
    FROM olympics_history
    GROUP BY Team
)
SELECT Team, Gold_Medals, Silver_Medals, Bronze_Medals
FROM MedalCount
WHERE Gold_Medals = 0
  AND (Silver_Medals > 0 OR Bronze_Medals > 0)
ORDER BY (Silver_Medals + Bronze_Medals) DESC;


WITH MedalCount AS (
    SELECT
        region AS Country,
        SUM(CASE WHEN UPPER(Medal) LIKE '%GOLD%'   THEN 1 ELSE 0 END)   AS Gold_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END)   AS Silver_Medals,
        SUM(CASE WHEN UPPER(Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END)   AS Bronze_Medals
    FROM olympics_history oh
    JOIN olympics_history_noc_regions ON oh.NOC = nr.NOC
    GROUP BY region
)
SELECT Country, Gold_Medals, Silver_Medals, Bronze_Medals
FROM MedalCount
WHERE Gold_Medals = 0
  AND (Silver_Medals > 0 OR Bronze_Medals > 0)
ORDER BY (Silver_Medals + Bronze_Medals) DESC;


WITH MedalCount AS (
  SELECT
    COALESCE(nr.region, oh.Team, oh.NOC) AS Country,
    SUM(CASE WHEN UPPER(oh.Medal) LIKE '%GOLD%'   THEN 1 ELSE 0 END) AS Gold_Medals,
    SUM(CASE WHEN UPPER(oh.Medal) LIKE '%SILVER%' THEN 1 ELSE 0 END) AS Silver_Medals,
    SUM(CASE WHEN UPPER(oh.Medal) LIKE '%BRONZE%' THEN 1 ELSE 0 END) AS Bronze_Medals
  FROM olympics_history AS oh
  LEFT JOIN olympics_history_noc_regions      AS nr ON oh.NOC = nr.NOC
  GROUP BY COALESCE(nr.region, oh.Team, oh.NOC)
)
SELECT Country, Gold_Medals, Silver_Medals, Bronze_Medals
FROM MedalCount
WHERE Gold_Medals = 0
  AND (Silver_Medals > 0 OR Bronze_Medals > 0)
ORDER BY (Silver_Medals + Bronze_Medals) DESC;

WITH IndiaMedals AS (
    SELECT 
        sport,
        COUNT(medal) AS total_medals
    FROM olympics_history
    WHERE noc = 'IND'   
      AND medal IS NOT NULL  
    GROUP BY sport
),
RankedSports AS (
    SELECT 
        sport,
        total_medals,
        RANK() OVER (ORDER BY total_medals DESC) AS rnk
    FROM IndiaMedals
)
SELECT sport, total_medals
FROM RankedSports
WHERE rnk = 1;

WITH India_Hockey_Medals AS (
    SELECT 
        games,             
        medal
    FROM olympics_history
    WHERE noc = 'IND'
      AND sport = 'Hockey'
      AND medal IS NOT NULL
)
SELECT 
    games,
    SUM(CASE WHEN medal LIKE '%Gold%' THEN 1 ELSE 0 END) AS gold_medals,
    SUM(CASE WHEN medal LIKE '%Silver%' THEN 1 ELSE 0 END) AS silver_medals,
    SUM(CASE WHEN medal LIKE '%Bronze%' THEN 1 ELSE 0 END) AS bronze_medals,
    COUNT(medal) AS total_medals
FROM India_Hockey_Medals
GROUP BY games
ORDER BY games;








    
