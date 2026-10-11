# data/source/ — immutable original dataset

The raw file is **not committed** (licensed/third-party Kaggle data; see `.gitignore`).

1. Download from https://www.kaggle.com/datasets/oktayrdeki/ddos-traffic-dataset (Kaggle login required).
2. Place the CSV here using the name in `config/settings.yml` (`dataset.source_file`, default `ddos_traffic.csv`),
   or set `SOURCE_FILE` in `.env`.
3. Verify identity: `python -m src.cli verify-source` must report the checksum recorded in
   `metadata/source_manifest.yml`.

Never edit this file. The pipeline only reads it.
