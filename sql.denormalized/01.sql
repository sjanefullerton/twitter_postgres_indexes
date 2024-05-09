SELECT COUNT(*) FROM tweets_jsonb
WHERE (data->'entities'->'hashtags'@@'$[*].text == "coronavirus"' OR data->'extended_tweet'->'hashtags'@@'$[*].text == "coronavirus"');
