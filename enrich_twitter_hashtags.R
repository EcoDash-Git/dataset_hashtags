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
twitter_raw <- DBI::dbReadTable(con, "twitter_raw")
cat("✓ downloaded", nrow(twitter_raw), "rows from twitter_raw\n")

## 2.5 – ensure tweet_url exists (fallback if missing) -----------------------
if (!"tweet_url" %in% names(twitter_raw)) {
  twitter_raw <- twitter_raw %>%
    mutate(tweet_url = paste0("https://twitter.com/", username, "/status/", tweet_id))
}

## 3 – canonical IDs ---------------------------------------------------------
main_ids <- tibble::tribble(
  ~username,            ~main_id,
  "weave_db",           "1206153294680403968",
  "OdyseeTeam",         "1280241715987660801",
  "ardriveapp",         "1293193263579635712",
  "redstone_defi",      "1294053547630362630",
  "everpay_io",         "1334504432973848577",
  "decentlandlabs",     "1352388512788656136",
  "KYVENetwork",        "136377177683878784",
  "onlyarweave",        "1393171138436534272",
  "ar_io_network",      "1468980765211955205",
  "Permaswap",          "1496714415231717380",
  "communitylabs",      "1548502833401516032",
  "usewander",          "1559946771115163651",
  "apus_network",       "1569621659468054528",
  "fwdresearch",        "1573616135651545088",
  "perma_dao",          "1595075970309857280",
  "Copus_io",           "1610731228130312194",
  "basejumpxyz",        "1612781645588742145",
  "AnyoneFDN",          "1626376419268784130",
  "arweaveindia",       "1670147900033343489",
  "useload",            "1734941279379759105",
  "protocolland",       "1737805485326401536",
  "aoTheComputer",      "1750584639385939968",
  "ArweaveOasis",       "1750723327315030016",
  "aox_xyz",            "1751903735318720512",
  "astrousd",           "1761104764899606528",
  "PerplexFi",          "1775862139980226560",
  "autonomous_af",      "1777500373378322432",
  "Liquid_Ops",         "1795772412396507136",
  "ar_aostore",         "1797632049202794496",
  "FusionFiPro",        "1865790600462921728",
  "vela_ventures",      "1869466343000444928",
  "beaconwallet",       "1879152602681585664",
  "VentoSwap",          "1889714966321893376",
  "permawebjournal",    "1901592191065300993",
  "Botega_AF",          "1902521779161292800",
  "samecwilliams",      "409642632",
  "TateBerenbaum",      "801518825690824707",
  "ArweaveEco",         "892752981736779776",
  "outprog_ar",         "2250655424",
  "HyMatrixOrg",        "1948283615248568320",
  "EverVisionLabs",     "1742119960535789568"
)


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





