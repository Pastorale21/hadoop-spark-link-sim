# Results Directory Scheme

`results/` 用于保存实验阶段的本地运行记录，和 Spark 实际输出目录分开管理。

建议结构：

```text
results/
├── README.md
└── runs/
    └── <run_id>_<dataset_name>/
        ├── command.txt
        ├── metadata.txt
        └── spark_submit.log
```

说明：

- `command.txt`
  保存本次实验实际使用的 `spark-submit` 命令，便于复现

- `metadata.txt`
  保存本次实验的关键参数与摘要结果

- `spark_submit.log`
  保存本次实验的标准输出和错误输出日志

推荐的 `metadata.txt` 字段：

```text
dataset_name=snap-web-NotreDame
master=yarn
deploy_mode=client
topk=20
max_referrers_per_dst=1000
write_intermediate=0
status=SUCCEEDED
start_time=2026-04-14 14:05:00 +0800
end_time=2026-04-14 14:19:42 +0800
duration_seconds=882
input_path=hdfs:///user/pastorale/linksim/input/snap/web-NotreDame.txt
output_path=hdfs:///user/pastorale/linksim/output/experiments/snap-web-NotreDame/20260414-140500
record_dir=/path/to/repo/results/runs/20260414-140500_snap-web-NotreDame
log_file=/path/to/repo/results/runs/20260414-140500_snap-web-NotreDame/spark_submit.log
```

推荐约定：

- Spark 真正的计算结果放在 `output/` 或 HDFS 输出目录
- `results/` 只放实验记录，不复制大型结果文件
- 每次正式实验使用唯一 `run_id`
- 对同一数据集做参数扫描时，优先比较 `metadata.txt` 中的 `topk`、`max_referrers_per_dst` 和 `duration_seconds`
