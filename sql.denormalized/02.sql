SELECT
    tag,
    COUNT(*) AS count
FROM (
    SELECT DISTINCT data->>'id' AS id_tweets,
    '#' || (jsonb_array_elements(data->'entities'->'hashtags' || 
            coalesce(data->'extended_tweet'->'entities'->'hashtags','[]'))->>'text') as tag
    FROM tweets_jsonb 
    WHERE data->'entities'->'hashtags' @@ '$[*].text == "coronavirus"'
    OR data->'extended_tweet'->'entities'->'hashtags' @@ '$[*].text == "coronavirus"'
) AS t
GROUP BY tag
ORDER BY count DESC, tag
LIMIT 1000;

