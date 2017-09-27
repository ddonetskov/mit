create table tweet (
    name          text, 
    tweet_text    text, 
    country_code  text, 
    display_url   text, 
    lang          text, 
    created_at    text, 
    location      text);

alter table tweet add column tweet_sentiment integer;
