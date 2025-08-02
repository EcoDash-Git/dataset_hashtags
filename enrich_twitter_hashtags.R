#!/usr/bin/env Rscript
# ────────────────────────────────────────────────────────────────
#  enrich_twitter_hashtags.R
#  – downloads  twitter_raw
#  – explodes every tweet into one row per hashtag
#  – uploads   public.twitter_hashtags   (OVERWRITE)
# ────────────────────────────────────────────────────────────────

## 0 – packages --------------------------------------------------------------
need <- c("DBI", "RPostgres", "dplyr", "stringr",
          "tibble", "tidyr", "lubridate")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new))
  install.packages(new, repos = "https://cloud.r-project.org", quiet = TRUE)
invisible(lapply(need, library, character.only = TRUE))

## 1 – Supabase credentials (from GH Secrets) -------------------------------
creds <- Sys.getenv(
  c("SUPABASE_HOST", "SUPABASE_PORT", "SUPABASE_DB",
    "SUPABASE_USER", "SUPABASE_PWD"),
  names = TRUE
)
if (any(!nzchar(creds)))
  stop("❌  One or more Supabase env vars are missing – aborting.")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = creds["SUPABASE_HOST"],
  port     = as.integer(creds["SUPABASE_PORT"]),
  dbname   = creds["SUPABASE_DB"],
  user     = creds["SUPABASE_USER"],
  password = creds["SUPABASE_PWD"],
  sslmode  = "require"
)

## 2 – download tweets -------------------------------------------------------
twitter_raw <- DBI::dbReadTable(con, "twitter_raw")
cat("✓ downloaded", nrow(twitter_raw), "rows from twitter_raw\n")

## 3 – canonical IDs ---------------------------------------------------------
main_ids <- tribble(
  ~username,            ~main_id,
  "weave_db",           "1206153294680403968",
  "OdyseeTeam",         "1280241715987660801",
  "ardriveapp",         "1293193263579635712",
  # … keep the rest unchanged …
  "ArweaveEco",         "892752981736779776"
)

## 4 – explode tweets → hashtags --------------------------------------------
hashtags <- twitter_raw %>%
  left_join(main_ids, by = "username") %>%
  mutate(publish_dt = ymd_hms(date, tz = "UTC")) %>%
  select(tweet_id, publish_dt, username, main_id, text) %>%
  mutate(tag = str_extract_all(text, "#\\w+")) %>%
  unnest(tag) %>%
  mutate(tag = str_to_lower(tag)) %>%          # normalise
  distinct(tweet_id, tag, .keep_all = TRUE)

cat("✓ extracted", nrow(hashtags), "hashtag rows\n")

## 5 – upload ---------------------------------------------------------------
dest_tbl <- "twitter_hashtags"   # keep the name you wanted

DBI::dbWriteTable(
  con,
  name   = dest_tbl,
  value  = as.data.frame(hashtags),
  overwrite = TRUE,
  row.names = FALSE
)
cat("✓ uploaded to table", dest_tbl, "\n")

DBI::dbDisconnect(con)
cat("✓ finished at", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
