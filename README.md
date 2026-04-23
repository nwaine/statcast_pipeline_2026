# Statcast Pipeline 2026 ⚾

End-to-end R pipeline for building and maintaining a local MLB Statcast database using Baseball Savant data.

---

## 🚀 Overview

This project provides a lightweight data pipeline that:

- Downloads pitch-level Statcast data from Baseball Savant
- Enriches data with `play_id` using the MLB Stats API
- Stores results in a local SQLite database
- Automatically appends new data on subsequent runs (no full re-download required)

---

## 🧱 How It Works

### Initial Run
- Pulls full-season Statcast data (season start → yesterday)
- Joins pitch-level data with Stats API identifiers
- Saves dataset to a SQLite database (`statcast.sqlite`)

### Future Runs
- Detects most recent `game_date` in the database
- Pulls only missing days
- Appends new data automatically

---

## 📂 Project Structure

```
statcast_pipeline_2026/
│
├── statcast_pipeline.R
├── statcast.sqlite        (ignored by git)
├── .gitignore
└── README.md
```

---

## ⚙️ Setup

Install required packages:

```r
install.packages(c("dplyr", "DBI", "RSQLite", "parallel", "tictoc"))

# Install sabRmetrics if needed
# install.packages("remotes")
# remotes::install_github("saberpowers/sabRmetrics")
```

---

## ▶️ Running the Pipeline

From R:

```r
source("statcast_pipeline.R")
```

- First run → builds full dataset
- Future runs → append new daily data automatically

---

## 🧠 Example Queries

### SQL

```r
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "statcast.sqlite")

dbGetQuery(con, "
  SELECT pitch_name, AVG(release_speed) AS avg_velocity
  FROM pitches
  GROUP BY pitch_name
")

dbDisconnect(con)
```

---

### dplyr

```r
library(dplyr)
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "statcast.sqlite")

tbl(con, "pitches") %>%
  count(pitch_name, sort = TRUE) %>%
  collect()

dbDisconnect(con)
```

---

## ⚡ Notes

- Deduplication key:
  game_id + at_bat_number + pitch_number

- Uses parallel processing for faster downloads

- SQLite database is excluded via `.gitignore`

---

## 📊 Data Sources

- Baseball Savant (Statcast)
- MLB Stats API

---

## 👤 Author

Nick Waine
