#!/usr/bin/env Rscript
# ────────────────────────────────────────────────────────────────
#  enrich_twitter_hashtags.R
#  – downloads  twitter_raw_plus_flags
#  – explodes every tweet into one row per hashtag
#  – uploads   public.twitter_hashtags   (OVERWRITE)
# ────────────────────────────────────────────────────────────────

## 0 – packages --------------------------------------------------------------
need <- c("DBI", "RPostgres", "dplyr", "stringr",
          "tibble", "tidyr", "lubridate", "purrr")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new))
  install.packages(new, repos = "https://cloud.r-project.org", quiet = TRUE)
invisible(lapply(need, library, character.only = TRUE))

## 1 – Supabase credentials (from GH Secrets) -------------------------------
creds <- Sys.getenv(
  c("SUPABASE_HOST","SUPABASE_PORT","SUPABASE_DB",
    "SUPABASE_USER","SUPABASE_PWD"),
  names = TRUE
)
if (any(!nzchar(creds)))
  stop("❌  One or more Supabase env vars are missing – aborting.")

# ── DB connection ───────────────────────────────────────────
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = creds[["SUPABASE_HOST"]],
  port     = as.integer(creds[["SUPABASE_PORT"]] %||% "6543"),
  dbname   = creds[["SUPABASE_DB"]],     # use your secret, not hard-coded
  user     = creds[["SUPABASE_USER"]],
  password = creds[["SUPABASE_PWD"]],
  sslmode  = "require"
)

`%||%` <- function(x, y) if (is.null(x) || identical(x, "") || is.na(x)) y else x

## 2 – download tweets -------------------------------------------------------
src_tbl <- "twitter_raw_plus_flags"
twitter_raw <- DBI::dbReadTable(con, src_tbl)
cat("✓ downloaded", nrow(twitter_raw), "rows from", src_tbl, "\n")

## 2.5 – ensure tweet_url exists (fallback if missing) -----------------------
if (!"tweet_url" %in% names(twitter_raw)) {
  twitter_raw <- twitter_raw %>%
    mutate(tweet_url = paste0("https://twitter.com/", username, "/status/", tweet_id))
}

## 3 – canonical IDs (only if missing) --------------------------------------
has_main <- "main_id" %in% names(twitter_raw)

if (!has_main) {
  main_map <- twitter_raw %>%
    filter(!is.na(user_id) & nzchar(user_id)) %>%
    count(username, user_id, name = "n") %>%
    group_by(username) %>%
    slice_max(n, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    rename(map_main_id = user_id) %>%
    select(username, map_main_id)
} else {
  main_map <- NULL
}

## 4 – explode tweets → hashtags (keep tweet_url) ---------------------------
hashtags <- twitter_raw %>%
  { if (!has_main) left_join(., main_map, by = "username") else mutate(., map_main_id = NA_character_) } %>%
  mutate(
    publish_dt = suppressWarnings(lubridate::ymd_hms(date, tz = "UTC")),
    main_id    = dplyr::coalesce(.data[["main_id"]], .data[["map_main_id"]], .data[["user_id"]]),
    text       = dplyr::coalesce(as.character(text), "")     # <-- vectorized NA → ""
  ) %>%
  select(dplyr::any_of(c("tweet_id","tweet_url","publish_dt","username","main_id","text","tweet_type"))) %>%
  mutate(tag = stringr::str_extract_all(text, "#\\w+")) %>%   # <-- no %||% here
  tidyr::unnest(tag, keep_empty = FALSE) %>%
  mutate(tag = stringr::str_to_lower(tag)) %>%
  distinct(tweet_id, tag, .keep_all = TRUE)



## 5 – upload ---------------------------------------------------------------
dest_tbl <- "twitter_hashtags"
DBI::dbWriteTable(
  con,
  name       = dest_tbl,
  value      = as.data.frame(hashtags),
  overwrite  = TRUE,
  row.names  = FALSE
)
cat("✓ uploaded to table", dest_tbl, "\n")

DBI::dbDisconnect(con)
cat("✓ finished at", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")









