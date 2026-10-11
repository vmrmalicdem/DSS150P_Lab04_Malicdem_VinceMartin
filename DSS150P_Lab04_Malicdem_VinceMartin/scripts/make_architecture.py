"""Regenerate docs/architecture.png (one-page lifecycle diagram). Run: python scripts/make_architecture.py"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parents[1] / "docs" / "architecture.png"

stages = [
    ("Source", "Kaggle DDoS\nTraffic CSV\n(immutable,\nSHA-256 pinned)", "#dbeafe", "Generation"),
    ("Ingestion", "full copy +\n4 deterministic\nbatches\ncheckpoint + dedup", "#dcfce7", "Ingestion"),
    ("Storage", "raw layer\ndata/raw/\n(later: Parquet,\nPostgreSQL)", "#fef9c3", "Storage"),
    ("Transformation", "staging ->\ncurated\n(typed, clean,\nmodel-ready)", "#fde2e2", "Transformation"),
    ("Training", "fixed seed,\nsplit manifest,\ndocumented\nhyperparameters", "#ede9fe", "Transformation"),
    ("Evaluation", "metrics.json vs\nexpected_metrics\n(tolerance check)", "#ffedd5", "Serving"),
    ("Outputs", "predictions.csv\nmodel/\nconsumer: reports", "#e2e8f0", "Serving"),
]

fig, ax = plt.subplots(figsize=(16, 6.2))
ax.set_xlim(0, 16)
ax.set_ylim(0, 6.2)
ax.axis("off")
ax.text(8, 5.85, "DSS150P Reproducible Predictive Pipeline — Lifecycle (DDoS Traffic, classification)",
        ha="center", fontsize=15, weight="bold")

w, h, gap, x0, y = 1.8, 2.0, 0.35, 0.3, 2.6
for i, (title, body, color, phase) in enumerate(stages):
    x = x0 + i * (w + gap)
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.12",
                                fc=color, ec="#334155", lw=1.4))
    ax.text(x + w / 2, y + h - 0.3, title, ha="center", fontsize=11.5, weight="bold")
    ax.text(x + w / 2, y + h / 2 - 0.22, body, ha="center", va="center", fontsize=9)
    ax.text(x + w / 2, y - 0.3, phase, ha="center", fontsize=8.5, style="italic", color="#475569")
    if i < len(stages) - 1:
        ax.add_patch(FancyArrowPatch((x + w + 0.03, y + h / 2), (x + w + gap - 0.03, y + h / 2),
                                     arrowstyle="-|>", mutation_scale=16, color="#334155", lw=1.4))

ax.text(0.35, 1.35, "Cross-cutting controls", fontsize=10.5, weight="bold")
ax.text(0.35, 0.95, "• metadata/: source manifest, profile, data contract, expected metrics, split manifest", fontsize=9)
ax.text(0.35, 0.6, "• state/: ingest checkpoint and per-run audit logs (run id, UTC time, source checksum)", fontsize=9)
ax.text(8.4, 0.95, "• Phase II+: Docker Compose, PostgreSQL, Airflow DAG (logic stays in src/)", fontsize=9)
ax.text(8.4, 0.6, "• Phase II+: data-quality checks, tests, lineage diagram, runbook", fontsize=9)

OUT.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(OUT, dpi=160, bbox_inches="tight")
print("wrote", OUT)
