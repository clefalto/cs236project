# Repo for Garett and Benedict's project for CS236 DBMS. 

Datasets not included in this repository to make it faster to download but you can download them [here](https://drive.google.com/file/d/1Dy6VHomaYtB4JVjPbr7wdu_lkBXE_TJq/view?usp=sharing)

## Phase 1 Instructions
- Note that this project requires Pyspark 4.2.1, Pandas, and matplotlib. All dependencies should be in the `requirements.txt` file, so you can run `pip install -r requirements.txt` to download all of them. I would also advise running the scripts in a Linux environment. We first tried to do it on Windows but we experienced some weird behavior when trying to save the CSVs.
- Download the datasets and run `raw_eda.py` to view some pre-clean statistics about the two datasets, such as the number of null values, the frequencies of request types, etc. We used many of these statistics when writing our report. 
-  Run `clean_la.py` and `clean_nyc.py` to clean the datasets. The cleaned CSVs will be written to the `cleaned/` directory in the root of the project. It'll be created if it doesn't exist. Now you can run `cleaned_eda.py` to view the same statistics from `raw_eda` on the cleaned datasets. 
- Once you've cleaned them, you can run `merge_datasets.py` to create the merged dataset in the `merged/` directory. 
- After merging, you can run `figs.py` to generate figures and `cleaned_eda.py` to see some statistics about the datasets

## Phase 2 Instructions
For running the analysis scripts:
- Run `phase2scripts/service_req_hotspots.py` to perform the zip code analysis. This script both outputs to the terminal and creates images in the project's root. 
- Run `phase2scripts/weekly_request_volume.py` to perform the weekly bucketing analysis. Once again, this script outputs to the terminal and creates images. 

For populating the database:
- This project uses Docker, so make sure you have it installed! The datasets are also pretty large so make sure you have adequate storage
- To create the empty Postgres container, `run docker compose up -d --build`. This also creates the three tables in the database: `la_clean`, `nyc_clean`, and `merged`.
- Then, run `phase2scripts/postgres.py` to actually populate the database.