import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

df = pd.read_csv("data/source/ddos_traffic.csv", low_memory=False)
df.columns = df.columns.str.strip()

# Test 1: does Source IP match the target?
print("Source IP vs target:")
print(pd.crosstab(df["Source IP"], df["target"]))

# Test 2: how well does each single feature predict the target?
num = df.select_dtypes("number").drop(columns=["target"])
for col in num.columns:
    Xtr, Xte, ytr, yte = train_test_split(
        num[[col]], df["target"], test_size=0.2, stratify=df["target"], random_state=42)
    acc = accuracy_score(yte, DecisionTreeClassifier(max_depth=5, random_state=42).fit(Xtr, ytr).predict(Xte))
    print(f"{col:15s} single-feature accuracy: {acc:.4f}")

# Test 3: score after removing duplicate rows before splitting
d = df.drop_duplicates()
X, y = d.drop(columns=["target"]), d["target"]
X = pd.get_dummies(X, columns=["Highest Layer", "Transport Layer", "Dest IP"])
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
acc = accuracy_score(yte, DecisionTreeClassifier(max_depth=10, random_state=42).fit(Xtr, ytr).predict(Xte))
print(f"\nDeduplicated accuracy: {acc:.4f}  ({len(d):,} unique rows)")