import json
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (accuracy_score, f1_score, precision_score,
                             recall_score, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier

SEED = 42
LEAK = ["Source IP"]
CAT = ["Highest Layer", "Transport Layer", "Dest IP"]

raw = pd.read_csv("data/source/ddos_traffic.csv", low_memory=False)
raw.columns = raw.columns.str.strip()

def run(df):
    y = df["target"]
    X = df.drop(columns=["target"] + LEAK)
    Xtr, Xte, ytr, yte = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=SEED)
    model = Pipeline([
        ("prep", ColumnTransformer(
            [("cat", OneHotEncoder(handle_unknown="ignore"), CAT)],
            remainder="passthrough")),
        ("clf", DecisionTreeClassifier(max_depth=10, random_state=SEED)),
    ])
    model.fit(Xtr, ytr)
    pred = model.predict(Xte)
    proba = model.predict_proba(Xte)[:, 1]
    m = {
        "accuracy": accuracy_score(yte, pred),
        "precision": precision_score(yte, pred),
        "recall": recall_score(yte, pred),
        "f1": f1_score(yte, pred),
        "roc_auc": roc_auc_score(yte, proba),
    }
    return {k: round(float(v), 4) for k, v in m.items()}

result = {
    "full_data_no_leak_column": run(raw),
    "deduplicated_no_leak_column": run(raw.drop_duplicates()),
}
Path("outputs").mkdir(exist_ok=True)
Path("outputs/baseline_clean_metrics.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))