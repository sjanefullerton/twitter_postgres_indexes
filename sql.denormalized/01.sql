SELECT COUNT(*) FROM tweets_jsonb
WHERE data ->'entities'->'hashtags' @@ '$[*].text == "coronavirus"' OR data->'entities'->'hashtags' @@ '$[*].text == "coronavirus"';
