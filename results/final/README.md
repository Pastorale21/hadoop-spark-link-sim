# Final Deliverables Scheme

`results/final/` 用于保存项目层面的最终成果，而不是单次实验的原始分片输出。

建议结构：

```text
results/final/
└── <dataset_name>/
    └── <run_id>/
        ├── export_summary.txt
        ├── topk_recommendations.txt
        ├── pair_similarity.txt
        ├── cleaned_edges.txt
        └── raw/
            └── <run_id>/
                ├── topk_recommendations/
                ├── pair_similarity/
                └── cleaned_edges/
```

实际是否会出现 `pair_similarity.txt` 和 `cleaned_edges.txt`，取决于运行时是否启用了 `--write-intermediate`。

说明：

- `topk_recommendations.txt`
  合并后的最终结果文件，适合作为项目展示成果

- `export_summary.txt`
  导出摘要，记录数据集名称、原始输出路径、本地保存路径和行数

- `raw/`
  保存 Spark 原始输出目录，便于核对 `_SUCCESS` 和 `part-*`

推荐使用：

```bash
bash scripts/export_final_result.sh
```

或者显式指定某次运行的输出路径：

```bash
bash scripts/export_final_result.sh hdfs:///user/$USER/linksim/output/experiments/snap-web-NotreDame/20260414-151446
```
