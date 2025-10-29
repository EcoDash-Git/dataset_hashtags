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

creds <- Sys.getenv(
  c("SUPABASE_HOST","SUPABASE_PORT","SUPABASE_DB",
    "SUPABASE_USER","SUPABASE_PWD"),
  names = TRUE
)

# ── DB connection ───────────────────────────────────────────
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = Sys.getenv("SUPABASE_HOST"),
  port     = as.integer(Sys.getenv("SUPABASE_PORT", "6543")),
  dbname   = "postgres",          # ← hard-coded DB name
  user     = Sys.getenv("SUPABASE_USER"),
  password = Sys.getenv("SUPABASE_PWD"),
  sslmode  = "require"
)



## 2 – download tweets -------------------------------------------------------
twitter_raw <- DBI::dbReadTable(con, "twitter_raw_plus_flags")
cat("✓ downloaded", nrow(twitter_raw), "rows from twitter_raw\n")

## 2.5 – ensure tweet_url exists (fallback if missing) -----------------------
if (!"tweet_url" %in% names(twitter_raw)) {
  twitter_raw <- twitter_raw %>%
    mutate(tweet_url = paste0("https://twitter.com/", username, "/status/", tweet_id))
}

## 3 – canonical IDs ---------------------------------------------------------
main_ids <- twitter_raw %>%
  filter(!is.na(user_id) & nzchar(user_id)) %>%
  group_by(username, user_id) %>%
  summarise(n = n(), .groups = "drop") %>%
  arrange(username, desc(n)) %>%
  group_by(username) %>%
  slice_max(n, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(username, main_id = user_id)


## 4 – explode tweets → hashtags (now keeping tweet_url) ---------------------
hashtags <- twitter_raw %>%
  left_join(main_ids, by = "username") %>%
  mutate(publish_dt = ymd_hms(date, tz = "UTC")) %>%
  select(tweet_id, tweet_url, publish_dt, username, main_id, text) %>%
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








