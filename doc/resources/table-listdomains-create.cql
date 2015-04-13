use crawler;

CREATE TABLE IF NOT EXISTS listdomains (
  domain text PRIMARY KEY,
  num_pages int
);
