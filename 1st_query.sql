CREATE TABLE
  myproject.mydataset.elon_musk_and_companies AS

SELECT
  *
FROM (
  SELECT
    EXTRACT (date
    FROM
      PARSE_TIMESTAMP('%Y%m%d%H%M%S',CAST(date AS string))) AS Date,
      
    SourceCommonName AS Source,
    
    DocumentIdentifier AS Link,
    
    CAST(SPLIT(V2Tone, ",") [
    OFFSET
      (0)] AS FLOAT64) AS Sentiment,
    CAST(SPLIT(V2Tone, ",") [
    OFFSET
      (3)] AS FLOAT64) AS Polarity,
    CAST(SPLIT(V2Tone, ",") [
    OFFSET
      (4)] AS FLOAT64) AS ARF,
      
    (CASE
        WHEN (
        LOWER(SourceCommonName) LIKE "%elon_musk%" 
        OR LOWER(SourceCommonName) LIKE "%elon-musk%" 
        OR LOWER(Persons) LIKE "elon musk" 
        OR LOWER(V2Persons) LIKE "elon musk" 
        OR LOWER(AllNames) LIKE "elon musk") THEN "Elon Musk"
        
        WHEN ( 
        (LOWER(SourceCommonName) LIKE "%tesla%"
          OR LOWER(Organizations) LIKE "tesla"
          OR LOWER(AllNames) LIKE "tesla")
        AND ( LOWER(SourceCommonName) NOT LIKE "%nikola%"
          OR LOWER(SourceCommonName) NOT LIKE "%coil%")) THEN "Tesla cars"
          
        WHEN ( 
        LOWER(SourceCommonName) LIKE "%openai%" 
        OR LOWER(SourceCommonName) LIKE "%open_ai%" 
        OR LOWER(SourceCommonName) LIKE "%open-ai%" 
        OR LOWER(Organizations) LIKE "openai" 
        OR LOWER(AllNames) LIKE "openai") THEN "OpenAI"
        
        WHEN ( 
        LOWER(SourceCommonName) LIKE "%spacex%"
        OR LOWER(SourceCommonName) LIKE "%starlink%"
        OR LOWER(SourceCommonName) LIKE "%space_x%"
        OR LOWER(SourceCommonName) LIKE "%space-x%"
        OR LOWER(Organizations) LIKE "spacex"
        OR LOWER(AllNames) LIKE "spacex"
        OR LOWER(AllNames) LIKE "starlink") THEN "SpaceX"
        
        WHEN ( 
        LOWER(SourceCommonName) LIKE "%hyperloop%" 
        OR LOWER(SourceCommonName) LIKE "%theboringcompany%" 
        OR LOWER(SourceCommonName) LIKE "%the-boring-company%" 
        OR LOWER(SourceCommonName) LIKE "%the_boring_company%" 
        OR LOWER(Organizations) LIKE "hyperloop" 
        OR LOWER(Organizations) LIKE "the boring company" 
        OR LOWER(AllNames) LIKE "the boring company") THEN "The Boring Company"
        
        WHEN (
        LOWER(SourceCommonName) LIKE "%neuralink%"
        OR LOWER(Organizations) LIKE "neuralink"
        OR LOWER(AllNames) LIKE "neuralink") THEN "Neuralink"
        
        WHEN (
        LOWER(SourceCommonName) LIKE "%paypal%" 
        OR LOWER(Organizations) LIKE "paypal" 
        OR LOWER(AllNames) LIKE "paypal") THEN "Paypal"
        
        WHEN ( 
        LOWER(SourceCommonName) LIKE "%solarcity%"
        OR LOWER(Organizations) LIKE "solarcity"
        OR LOWER(AllNames) LIKE "solarcity") THEN "SolarCity"
        
    END
      ) AS Mentions,
  FROM
    `gdelt-bq.gdeltv2.gkg_partitioned` )
WHERE
  Mentions IS NOT NULL