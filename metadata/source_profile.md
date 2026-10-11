# Source Profile

- **Dataset:** DDoS Traffic (Network Traffic Dataset Under DDoS Attack) (`kaggle-oktayrdeki-ddos-traffic-dataset`)
- **Source:** https://www.kaggle.com/datasets/oktayrdeki/ddos-traffic-dataset — owner oktayrdeki (Kaggle)
- **File:** `data/source/ddos_traffic.csv` · csv · encoding utf-8 · delimiter `,` · 37,685,016 bytes
- **SHA-256:** `f9242444870e2a33e81218f1c8ec54e4deb315b15da7cb420ace598214bdc504`
- **Rows × columns:** 852,585 × 9
- **Duplicate rows (all columns):** 766,840
- **Duplicate business-key rows:** n/a (no business key configured)
- **Update frequency / availability:** static Kaggle snapshot; no known update schedule.
- **License / redistribution:** check the dataset page; raw file is not published to GitHub.

## Target variable
- Detected target: `target`

| Class | Count | Share |
|---|---:|---:|
| 0 | 454,849 | 53.35% |
| 1 | 397,736 | 46.65% |

## Columns

| Column | dtype | nulls | null % | distinct | min | max | flags |
|---|---|---:|---:|---:|---|---|---|
| Highest Layer | str | 0 | 0.00% | 10 |  |  |  |
| Transport Layer | str | 0 | 0.00% | 2 |  |  |  |
| Source IP | int64 | 0 | 0.00% | 2 | 0 | 1 | identifier/leakage review, leaks target |
| Dest IP | str | 0 | 0.00% | 17 |  |  | identifier/leakage review |
| Source Port | int64 | 0 | 0.00% | 649 | 0 | 65530 | identifier/leakage review |
| Dest Port | int64 | 0 | 0.00% | 14 | 0 | 50890 | identifier/leakage review |
| Packet Length | int64 | 0 | 0.00% | 81 | 42 | 1514 |  |
| Packets/Time | float64 | 0 | 0.00% | 4,843 | 0 | 484.4 |  |
| target | int64 | 0 | 0.00% | 2 | 0 | 1 |  |

## Risks and review items
- Identifier / leakage candidates to review before modelling: Source IP, Dest IP, Source Port, Dest Port.
- Timestamp-like columns: none (not assumed reliable; ingestion uses a row-sequence simulation unless `timestamp_column` is set).
- Columns containing inf: none.
- Columns with nulls: none.
- Columns that perfectly determine the target (label leakage, excluded from modelling): Source IP.
