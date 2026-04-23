library(sabRmetrics)
library(dplyr)
library(DBI)
library(RSQLite)
library(parallel)
library(tictoc)

# -----------------------------
# SETTINGS
# -----------------------------
SEASON_START <- as.Date("2026-03-25")
DB_PATH      <- "statcast.sqlite"
TABLE_NAME   <- "pitches"
USE_CLUSTER  <- TRUE
N_WORKERS    <- 6

# -----------------------------
# CLUSTER
# -----------------------------
make_statcast_cluster <- function() {
  if (!USE_CLUSTER) return(NULL)
  makeCluster(N_WORKERS)
}

# -----------------------------
# DB CONNECTION
# -----------------------------
connect_db <- function() {
  dbConnect(SQLite(), DB_PATH)
}

# -----------------------------
# ATTACH play_id
# -----------------------------
attach_play_id <- function(savant_df, pbp_obj) {
  mlb_pbp <- pbp_obj[["pitch"]] %>%
    select(
      game_id,
      at_bat_number = event_index,
      pitch_number,
      play_id
    ) %>%
    mutate(at_bat_number = at_bat_number + 1)
  
  left_join(savant_df, mlb_pbp,
            by = c("game_id","at_bat_number","pitch_number"))
}

# -----------------------------
# INITIAL BUILD
# -----------------------------
build_full_dataset <- function(cluster) {
  
  end_date <- Sys.Date() - 1
  
  tic("Initial build")
  
  savant <- sabRmetrics::download_baseballsavant(
    start_date = SEASON_START,
    end_date   = end_date,
    cl = cluster
  )
  
  pbp <- sabRmetrics::download_statsapi(
    SEASON_START,
    end_date,
    level = "MLB",
    game_type = "R",
    cl = cluster
  )
  
  toc()
  
  savant <- attach_play_id(savant, pbp) %>%
    distinct(game_id, at_bat_number, pitch_number, .keep_all = TRUE) %>%
    arrange(game_date, game_id, at_bat_number, pitch_number)
  
  savant
}

# -----------------------------
# APPEND MISSING DAYS
# -----------------------------
append_missing_days_db <- function(con, cluster) {
  
  last_date <- dbGetQuery(con, paste0("
    SELECT MAX(game_date) AS last_date FROM ", TABLE_NAME
  ))$last_date
  
  if (is.na(last_date)) {
    stop("Database empty. Run initial build first.")
  }
  
  last_date <- as.Date(last_date)
  start_missing <- last_date + 1
  end_missing   <- Sys.Date() - 1
  
  if (start_missing > end_missing) {
    message("No new data to append.")
    return()
  }
  
  message(paste("Pulling:", start_missing, "to", end_missing))
  
  tic("Appending new data")
  
  savant_new <- sabRmetrics::download_baseballsavant(
    start_date = start_missing,
    end_date   = end_missing,
    cl = cluster
  )
  
  pbp_new <- sabRmetrics::download_statsapi(
    start_missing,
    end_missing,
    level = "MLB",
    game_type = "R",
    cl = cluster
  )
  
  toc()
  
  savant_new <- attach_play_id(savant_new, pbp_new) %>%
    distinct(game_id, at_bat_number, pitch_number, .keep_all = TRUE)
  
  dbWriteTable(con, TABLE_NAME, savant_new, append = TRUE)
}

# -----------------------------
# MAIN PIPELINE
# -----------------------------
run_pipeline <- function() {
  
  cluster <- make_statcast_cluster()
  con <- connect_db()
  
  if (!TABLE_NAME %in% dbListTables(con)) {
    
    message("No table found. Running initial build...")
    
    savant <- build_full_dataset(cluster)
    
    dbWriteTable(con, TABLE_NAME, savant, overwrite = TRUE)
    
    dbExecute(con, paste0("
      CREATE INDEX idx_pitch_key
      ON ", TABLE_NAME, "(game_id, at_bat_number, pitch_number)
    "))
    
  } else {
    
    message("Table exists. Appending missing days...")
    append_missing_days_db(con, cluster)
  }
  
  dbDisconnect(con)
  if (!is.null(cluster)) stopCluster(cluster)
  
  message("Pipeline complete.")
}

# -----------------------------
# RUN IT
# -----------------------------
run_pipeline()
