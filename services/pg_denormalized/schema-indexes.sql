CREATE INDEX idx_hashtags_coronavirus_entities ON tweets_jsonb USING GIN ((data->'entities'->'hashtags'));
CREATE INDEX idx_hashtags_coronavirus_ext ON tweets_jsonb USING GIN ((data->'extended_tweet'->'hashtags')); 
CREATE INDEX idx_text_search_extended ON tweets_jsonb USING GIN (to_tsvector('english', COALESCE(data->'extended_tweet'->>'full_text',data->>'text')));
CREATE INDEX idx_hashtags_coronavirus_extended ON tweets_jsonb USING GIN ((data->'extended_tweet'->'entities'->'hashtags'));
CREATE INDEX idx_hashtags_coronavirus_extended_lang ON tweets_jsonb USING GIN ((data->'extended_tweet'->'entities'->'hashtags'));
CREATE INDEX idx_lang ON tweets_jsonb ((data->>'lang'));
