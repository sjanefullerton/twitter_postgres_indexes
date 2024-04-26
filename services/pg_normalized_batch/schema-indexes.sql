CREATE INDEX idx_tag_id_tweets ON tweet_tags(tag, id_tweets);

CREATE INDEX idx_tag_id_tweets_new ON tweet_tags(tag, id_tweets);

CREATE INDEX idx_tag_id_tweets_3 ON tweet_tags(tag, id_tweets);
CREATE INDEX idx_id_tweets_3 ON tweets(id_tweets);

CREATE INDEX idx_lang_4 ON tweets(lang);
CREATE INDEX idx_text_tsv_4 ON tweets USING gin(to_tsvector('english', text));

CREATE INDEX idx_lang_5 ON tweets(lang);
CREATE INDEX idx_text_tsv_5 ON tweets USING gin(to_tsvector('english', text));
CREATE INDEX idx_id_tweets_5 ON tweet_tags(id_tweets);
CREATE INDEX idx_tag_5 ON tweet_tags(tag);
