"""One-off baseline run: trains a simple model and writes metrics."""
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
df = pd.read_csv("data/source/ddos_traffic.csv", low_memory=False)
df.columns = df.columns.str.strip()

y = df["target"]
X = df.drop(columns=["target"])
cat = ["Highest Layer", "Transport Layer", "Dest IP"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=SEED
)

model = Pipeline([
    ("prep", ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), cat)],
        remainder="passthrough")),
    ("clf", DecisionTreeClassifier(max_depth=10, random_state=SEED)),
])
model.fit(X_train, y_train)

pred = model.predict(X_test)
proba = model.predict_proba(X_test)[:, 1]
metrics = {
    "accuracy": accuracy_score(y_test, pred),
    "precision": precision_score(y_test, pred),
    "recall": recall_score(y_test, pred),
    "f1": f1_score(y_test, pred),
    "roc_auc": roc_auc_score(y_test, proba),
}
metrics = {k: round(float(v), 4) for k, v in metrics.items()}

Path("outputs").mkdir(exist_ok=True)
Path("outputs/baseline_metrics.json").write_text(json.dumps(metrics, indent=2))
print(json.dumps(metrics, indent=2))