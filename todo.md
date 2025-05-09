# Movie Recommendation Data Pipeline - TODO Checklist

## Iteration 1: Foundation

*   [x] **1.1: Project Setup - Initial Structure**
    *   [x] Initialize Git repository (`git init`).
    *   [x] Create root directory (`movie-recommendation-pipeline/`).
    *   [x] Create subdirectories: `src/`, `tests/`, `logs/`, `models/`.
    *   [x] Create empty files: `main.py`, `requirements.txt`, `config.yaml`, `.gitignore`, `README.md`.
    *   [x] Create empty files in `src/`: `__init__.py`, `utils.py`, `data_loader.py`, `cleaner.py`, `feature_engineer.py`, `model.py`, `writer.py`.
    *   [x] Create empty file in `tests/`: `__init__.py`.
*   [x] **1.2: Project Setup - .gitignore**
    *   [x] Populate `.gitignore` with Python, IDE, OS, logs, models, venv, secrets exclusions.
*   [x] **1.3: Project Setup - requirements.txt**
    *   [x] Populate `requirements.txt` with `pyspark`, `pyyaml`, `psycopg2-binary`, `pytest`, `boto3`.
    *   [x] Create and activate a Python virtual environment.
    *   [x] Install requirements (`pip install -r requirements.txt`).
*   [x] **1.4: Project Setup - config.yaml**
    *   [x] Populate `config.yaml` with structure (s3, postgres, als, output, logging).
    *   [x] Use placeholder values (esp. for secrets).
    *   [x] Set initial `s3.paths` to use *local* relative paths (e.g., `data/movies.csv`) for easier development.
    *   [x] Create a local `data/` directory and place sample/full MovieLens CSVs inside.
*   [x] **1.5: Core Utilities - Config Loading**
    *   [x] Implement `load_config(path)` function in `src/utils.py` (import yaml, error handling).
    *   [x] Create `tests/test_utils.py`.
    *   [x] Write `test_load_config` using `tmp_path` and dummy YAML.
    *   [x] Write test for `FileNotFoundError` case using `pytest.raises`.
*   [x] **1.6: Core Utilities - Logging Setup**
    *   [x] Implement `setup_logging(config)` function in `src/utils.py` (import logging, configure basicConfig with FileHandler and StreamHandler, create log dir).
    *   [x] Write `test_setup_logging` in `tests/test_utils.py` (check logger config, file creation, log message).
*   [x] **1.7: Main Script - Initial Integration**
    *   [x] Update `main.py` to import utils.
    *   [x] Implement main block: load config, setup logging, log start/finish messages.
    *   [x] Add top-level try/except block for setup errors.
    *   [x] Run `python main.py` to verify basic setup.

## Iteration 2: Spark Session & Basic Loading

*   [x] **2.1: Core Utilities - Spark Session**
    *   [x] Implement `get_spark_session()` in `src/utils.py` (import SparkSession, basic local config, add `spark.jars.packages` for PostgreSQL driver).
    *   [x] Write `test_get_spark_session` in `tests/test_utils.py` (check instance type, app name, master, stop session).
*   [ ] **2.2: Data Loading - CSV Loader Function**
    *   [ ] Implement `load_csv_data(spark, path)` in `src/data_loader.py` (use `spark.read.csv`, logging, error handling).
*   [ ] **2.3: Data Loading - CSV Loader Test**
    *   [ ] Create `tests/test_data_loader.py`.
    *   [ ] Create `tests/data/` directory.
    *   [ ] Create `tests/data/sample_movies.csv`.
    *   [ ] Implement `spark_session` fixture (scope="session", yield session, stop session).
    *   [ ] Write `test_load_csv_data` using the fixture and sample CSV (check count, schema, types).
    *   [ ] Write `test_load_csv_data_not_found` using `pytest.raises(AnalysisException)`.
*   [ ] **2.4: Main Script - Integrate Spark and Load One Dataset**
    *   [ ] Update `main.py` to import `get_spark_session`, `load_csv_data`.
    *   [ ] Get SparkSession in `main`.
    *   [ ] Add `try...finally` block to ensure `spark.stop()`.
    *   [ ] Inside `try`, load *movies* dataset using config path and `load_csv_data`.
    *   [ ] Log loaded DataFrame schema and count.

## Iteration 3: Cleaning Functions (Incremental)

*   [ ] **3.1: Cleaning - Movies Data Function**
    *   [ ] Implement `clean_movies(df)` in `src/cleaner.py` (drop null `movieId`, split `genres` to array).
*   [ ] **3.2: Cleaning - Movies Data Test**
    *   [ ] Create `tests/test_cleaner.py`.
    *   [ ] Write `test_clean_movies` using sample DataFrame (assert null drops, genre array type, content).
*   [ ] **3.3: Cleaning - Ratings Data Function**
    *   [ ] Implement `clean_ratings(df)` in `src/cleaner.py` (filter rating range 0.5-5.0, drop null userId/movieId/rating).
*   [ ] **3.4: Cleaning - Ratings Data Test**
    *   [ ] Write `test_clean_ratings` in `tests/test_cleaner.py` using sample DataFrame (assert filtering and null drops).
*   [ ] **3.5: Cleaning - Tags Data Function**
    *   [ ] Implement `clean_tags(df)` in `src/cleaner.py` (drop null userId/movieId, filter null/empty tag, drop duplicates).
*   [ ] **3.6: Cleaning - Tags Data Test**
    *   [ ] Write `test_clean_tags` in `tests/test_cleaner.py` using sample DataFrame (assert filtering and duplicate drops).
*   [ ] **3.7: Cleaning - Links Data Function**
    *   [ ] Implement `clean_links(df)` in `src/cleaner.py` (drop null movieId/imdbId/tmdbId).
*   [ ] **3.8: Cleaning - Links Data Test**
    *   [ ] Write `test_clean_links` in `tests/test_cleaner.py` using sample DataFrame (assert null drops).
*   [ ] **3.9: Main Script - Integrate All Cleaning Steps**
    *   [ ] Update `main.py` to import all cleaners.
    *   [ ] Load ratings, tags, links datasets.
    *   [ ] Call corresponding cleaning function for each loaded DataFrame.
    *   [ ] Log counts before/after cleaning for each. Add null checks after loading.

## Iteration 4: Feature Engineering (Joining)

*   [ ] **4.1: Feature Engineering - Basic Join Function**
    *   [ ] Create `src/feature_engineer.py`.
    *   [ ] Implement `join_dataframes(ratings_df, movies_df)` (inner join on `movieId`, select columns).
*   [ ] **4.2: Feature Engineering - Basic Join Test**
    *   [ ] Create `tests/test_feature_engineer.py`.
    *   [ ] Write `test_join_dataframes_basic` using sample cleaned DFs (assert count, schema, values).
*   [ ] **4.3: Feature Engineering - Optional Joins**
    *   [ ] Modify `join_dataframes` signature for optional `tags_df`, `links_df`.
    *   [ ] Implement left joins for tags (on userId, movieId) and links (on movieId) if DFs provided.
    *   [ ] Update final column selection.
*   [ ] **4.4: Feature Engineering - Optional Join Tests**
    *   [ ] Write `test_join_dataframes_with_tags` (check count, schema, tag values/nulls).
    *   [ ] Write `test_join_dataframes_all` (check count, schema, values from all sources).
*   [ ] **4.5: Main Script - Integrate Joining**
    *   [ ] Update `main.py` to import `join_dataframes`.
    *   [ ] Call `join_dataframes` with cleaned DFs (ratings, movies, tags, links).
    *   [ ] Log confirmation and resulting DataFrame info.

## Iteration 5: ALS Model Training

*   [ ] **5.1: Model Training - ALS Function**
    *   [ ] Create `src/model.py`.
    *   [ ] Implement `train_als_model(data_df, config)` (import ALS, set params from config, set cols, coldStartStrategy, nonnegative, fit model, error handling).
*   [ ] **5.2: Model Training - ALS Test**
    *   [ ] Create `tests/test_model.py`.
    *   [ ] Write `test_train_als_model` using small sample data and dummy config (assert model type is `ALSModel`, basic transform works).
*   [ ] **5.3: Main Script - Integrate Model Training**
    *   [ ] Update `main.py` to import `train_als_model`.
    *   [ ] Call `train_als_model` with joined data and config.
    *   [ ] Log confirmation.
    *   [ ] Optional: Save model using `model.save("models/als_model")`. Ensure `models/` exists.

## Iteration 6: Generating Recommendations

*   [ ] **6.1: Recommendations - Generation Function**
    *   [ ] Implement `generate_recommendations(model, top_n)` in `src/model.py` (use `recommendForAllUsers`, explode results, select final columns `userId`, `movieId`, `predicted_rating`).
*   [ ] **6.2: Recommendations - Generation Test**
    *   [ ] Write `test_generate_recommendations` in `tests/test_model.py` (train model on sample data, generate recs, assert schema, count per user <= top_n, check rating type).
*   [ ] **6.3: Main Script - Integrate Recommendation Generation**
    *   [ ] Update `main.py` to import `generate_recommendations`.
    *   [ ] Call `generate_recommendations` with trained model and `top_n` from config.
    *   [ ] Log confirmation.

## Iteration 7: Writing to PostgreSQL

*   [ ] **7.1: Data Writing - PostgreSQL Writer Function**
    *   [ ] Create `src/writer.py`.
    *   [ ] Implement `write_to_postgres(df, db_config, table_name)` (build JDBC URL, properties, use `df.write.jdbc`, mode='overwrite', error handling).
*   [ ] **7.2: Data Writing - PostgreSQL Writer Test**
    *   [ ] Set up a local **test** PostgreSQL database/user.
    *   [ ] Create `tests/test_writer.py`.
    *   [ ] Define test DB config (use env vars or safe method).
    *   [ ] Write `test_write_to_postgres` (write sample DF, read back via JDBC, assert count/content, test overwrite, cleanup table).
*   [ ] **7.3: Main Script - Integrate Writing Recommendations**
    *   [ ] Update `main.py` to import `write_to_postgres`.
    *   [ ] Call `write_to_postgres` for `recommendations_df` using DB config and table name from main config.
    *   [ ] Log confirmation.

## Iteration 8: Adding Movie Stats

*   [ ] **8.1: Feature Engineering - Movie Stats Function**
    *   [ ] Implement `calculate_movie_stats(ratings_df)` in `src/feature_engineer.py` (group by `movieId`, agg `avg(rating)`, `count(rating)`).
*   [ ] **8.2: Feature Engineering - Movie Stats Test**
    *   [ ] Write `test_calculate_movie_stats` in `tests/test_feature_engineer.py` (use sample ratings, assert stats for known movie IDs, assert total count).
*   [ ] **8.3: Main Script - Integrate Movie Stats Calc & Write**
    *   [ ] Update `main.py` to import `calculate_movie_stats`.
    *   [ ] Call `calculate_movie_stats` using `cleaned_ratings_df`.
    *   [ ] Call `write_to_postgres` for `movie_stats_df` using DB config and table name from main config.
    *   [ ] Log confirmation.

## Iteration 9: S3 Integration & Final Polish

*   [ ] **9.1: S3 Config & Spark Session Update**
    *   [ ] Update `config.yaml` paths to use `s3a://` URIs.
    *   [ ] Update `get_spark_session` in `src/utils.py` to add `hadoop-aws` JAR to `spark.jars.packages`.
    *   [ ] Add comments in `get_spark_session` explaining S3 credential configuration (env vars, SparkConf).
*   [ ] **9.2: S3 Testing Considerations**
    *   [ ] Manually test pipeline against actual S3 bucket (requires data upload & AWS credential setup).
    *   [ ] Add notes to relevant tests (`test_data_loader.py`) about S3 testing requiring mocks (`moto`) or integration setup.
*   [ ] **9.4: Logging Review**
    *   [ ] Review all `logging.info/warning/error` messages throughout the code for clarity and usefulness.
*   [ ] **9.5: Documentation - README.md**
    *   [ ] Populate `README.md` with all sections (Overview, Stack, Structure, Setup, Config, Run, Test, Enhancements). Use Markdown formatting.
*   [ ] **9.6: Final Testing**
    *   [ ] Run `pytest` and ensure all tests pass (including PostgreSQL integration tests if feasible).
    *   [ ] Run the full pipeline `python main.py` using S3 paths (if configured) and verify output in PostgreSQL.

## Post-Completion

*   [ ] Review code for style consistency and best practices.
*   [ ] Add more robust error handling where needed.
*   [ ] Consider adding type checking tools (e.g., mypy).
*   [ ] Commit final changes to Git.
