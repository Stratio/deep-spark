use crawler;

CREATE TABLE IF NOT EXISTS "Page" (
  key text PRIMARY KEY,
  "___class" text,
  charset text,
  content text,
  "domainName" text,
  "downloadTime" bigint,
  "enqueuedForTransforming" bigint,
  etag text,
  "firstDownloadTime" bigint,
  "lastModified" text,
  "responseCode" varint,
  "responseTime" bigint,
  "timeTransformed" bigint,
  title text,
  url text
);

