SELECT
    lang,
    COUNT(*) AS count
FROM (
    SELECT DISTINCT data->>'id' AS id_tweets, data->>'lang' AS lang
    FROM tweets_jsonb
    WHERE data->'entities'->'hashtags' @@ '$[*].text == "coronavirus"' OR data->'extended_tweet'->'entities'->'hashtags' @@ '$[*].text == "coronavirus"'
) t
GROUP BY lang
ORDER BY count DESC, lang;
