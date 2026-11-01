# Repo for Garett and Benedict's project for CS236 DBMS. 

Datasets not included in this repository to make it faster to download but you can download them [here](https://drive.google.com/file/d/1Dy6VHomaYtB4JVjPbr7wdu_lkBXE_TJq/view?usp=sharing)

### Instructions
- Note that this project requires Pyspark 4.2.1, Pandas, and matplotlib. All dependencies should be in the `requirements.txt` file, so you can run `pip install -r requirements.txt` to download all of them. 
- Download the datasets and run `clean_la.py` and `clean_nyc.py` to clean them. These will be written to the `cleaned/` directory in the root of the project. It'll be created if it doesn't exist.
- Once you've cleaned them, you can run `merge_datasets.py` to create the merged dataset in the `merged/` directory. 
- After merging, you can run `figs.py` to generate figures and `partial_eda.py` to see some statistics about the datasets, such as the number of null values, the frequencies of request types, etc. We used most of these statistics when writing our report in one way or another. 