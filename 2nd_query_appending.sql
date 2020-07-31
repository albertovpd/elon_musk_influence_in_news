
  -- I am using BigQuery scheduling options to monthly run this query and append to the
  -- Elon Musk Table. That table will be the source of the Data studio.

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
      
      --
      
    (CASE
        WHEN (
        LOWER(DocumentIdentifier) LIKE "%elon_musk%" 
        OR LOWER(DocumentIdentifier) LIKE "%elon-musk%"
        OR LOWER(DocumentIdentifier) LIKE "%elonmusk%"
        
        OR LOWER(Persons) LIKE "%elon musk%" 
        OR LOWER(V2Persons) LIKE "%elon musk%" 
        OR LOWER(AllNames) LIKE "%elon musk%") THEN "Elon Musk"
        
        --
        
        WHEN (
         
        (LOWER(DocumentIdentifier) LIKE "%tesla%"
          OR LOWER(Organizations) LIKE "%tesla%"
          OR LOWER(AllNames) LIKE "%tesla%")
          
        AND ( 
        LOWER(DocumentIdentifier) NOT LIKE "%nikola%"
          OR LOWER(AllNames) NOT LIKE "%nikola%"
          OR LOWER(DocumentIdentifier) NOT LIKE "%coil%")
          
          or LOWER(SourceCommonName) like "%tesla%" and LOWER(SourceCommonName) not like "%nikola%"
          
          ) THEN "Tesla"
          
          --
          
        WHEN ( 
        LOWER(DocumentIdentifier) LIKE "%openai%" 
        OR LOWER(DocumentIdentifier) LIKE "%open_ai%" 
        OR LOWER(DocumentIdentifier) LIKE "%open-ai%"
        OR LOWER(DocumentIdentifier) LIKE "%robosumo%"
        OR LOWER(DocumentIdentifier) LIKE "%dactyl%"
        OR LOWER(DocumentIdentifier) LIKE "%openai_gym%"
        OR LOWER(DocumentIdentifier) LIKE "%openai-gym%"
        OR LOWER(DocumentIdentifier) LIKE "%openaigym%"
        
        OR LOWER(SourceCommonName) LIKE "%openai.com%"
        OR LOWER(Organizations) LIKE "%openai%" 
        OR LOWER(AllNames) LIKE "%openai%"
        
        or LOWER(SourceCommonName) like "%openai%"
        
        ) THEN "OpenAI"
        
        --
        
        WHEN ( 
        LOWER(DocumentIdentifier) LIKE "%spacex%"
        or LOWER(SourceCommonName) like "%spacex%"
        OR LOWER(DocumentIdentifier) LIKE "%starlink%"
        OR LOWER(DocumentIdentifier) LIKE "%space_x%"
        OR LOWER(DocumentIdentifier) LIKE "%space-x%"
        OR LOWER(Organizations) LIKE "spacex"
        OR LOWER(AllNames) LIKE "spacex"
        OR LOWER(AllNames) LIKE "starlink") THEN "SpaceX"
        
        --
        
        WHEN ( 
        LOWER(DocumentIdentifier) LIKE "%hyperloop%" 
        OR LOWER(DocumentIdentifier) LIKE "%theboringcompany%" 
        OR LOWER(DocumentIdentifier) LIKE "%the-boring-company%" 
        OR LOWER(DocumentIdentifier) LIKE "%the_boring_company%" 
        
        OR LOWER(DocumentIdentifier) LIKE "%las_vegas_convention_center_loop%"
        OR LOWER(DocumentIdentifier) LIKE "%las-vegas-convention-center-loop%"
        
        OR LOWER(DocumentIdentifier) LIKE "%dogout_loop%"
        OR LOWER(DocumentIdentifier) LIKE "%dogout-loop%"
        
        OR LOWER(DocumentIdentifier) LIKE "%baltimore_loop%"
        OR LOWER(DocumentIdentifier) LIKE "%baltimore-loop%"
        
        
        OR LOWER(Organizations) LIKE "%hyperloop%" 
        OR LOWER(Organizations) LIKE "%las vegas convention center loop%"
        OR LOWER(Organizations) LIKE "%dogout loop%"
        OR LOWER(Organizations) LIKE "%baltimore loop%" 
        OR LOWER(Organizations) LIKE "%the boring company%" 
        OR LOWER(AllNames) LIKE "%the boring company%"
        OR LOWER(AllNames) LIKE "%hyperloop%"
        OR LOWER(AllNames) LIKE "%las vegas convention center loop%"
        OR LOWER(AllNames) LIKE "%dogout loop%"
        OR LOWER(AllNames) LIKE "%baltimore loop%"
        
        or LOWER(SourceCommonName) like "%theboringcompany%"
        
        ) THEN "The Boring Company"
        
        --
        
        WHEN (
        LOWER(DocumentIdentifier) LIKE "%neuralink%"
        OR LOWER(Organizations) LIKE "%neuralink%"
        OR LOWER(AllNames) LIKE "%neuralink%"
        
        or LOWER(SourceCommonName) like "%spacex%"
        ) THEN "Neuralink"
        
        WHEN (
        LOWER(DocumentIdentifier) LIKE "%paypal%" 
        OR LOWER(DocumentIdentifier) LIKE "%krakenjs%"
        OR LOWER(DocumentIdentifier) LIKE "%nemojs%"
        OR (LOWER(DocumentIdentifier) LIKE "%selion%" AND LOWER(DocumentIdentifier) LIKE "%java%")
        OR (LOWER(DocumentIdentifier) LIKE "%gimel%" AND LOWER(DocumentIdentifier) LIKE "%data%")
        OR (LOWER(DocumentIdentifier) LIKE "%squbs%" and LOWER(DocumentIdentifier) LIKE "%deploy%")
        OR LOWER(DocumentIdentifier) LIKE "%card.io%"
        OR LOWER(DocumentIdentifier) LIKE "%card_io%"
        OR LOWER(DocumentIdentifier) LIKE "%card-io%"
        OR (LOWER(DocumentIdentifier) LIKE "%namenode%" and LOWER(DocumentIdentifier) LIKE "%analytics%")
        OR LOWER(Organizations) LIKE "%paypal%" 
        OR LOWER(AllNames) LIKE "%paypal%"
        or LOWER(SourceCommonName) like "%spacex%"
        ) THEN "PayPal"
        
        WHEN ( 
        LOWER(DocumentIdentifier) LIKE "%solarcity%"
        OR LOWER(Organizations) LIKE "%solarcity%"
        OR LOWER(Organizations) LIKE "%solar city%"
        OR LOWER(AllNames) LIKE "%solarcity%"
        OR LOWER(AllNames) LIKE "%solar city%"
        
        OR (
        LOWER(DocumentIdentifier) LIKE "%tesla%" 
        AND 
        (
        LOWER(DocumentIdentifier) LIKE "%batery%"
        OR LOWER(DocumentIdentifier) LIKE "%bateries%"
        OR LOWER(DocumentIdentifier) LIKE "%autonomy%")
        )
        or LOWER(SourceCommonName) like "%solarcity%"
        ) THEN "SolarCity"
        
    END
      ) AS Mentions,
  FROM
    `gdelt-bq.gdeltv2.gkg_partitioned` )

WHERE
  Mentions IS NOT NULL
  AND Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE() 
