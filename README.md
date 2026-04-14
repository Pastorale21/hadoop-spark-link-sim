# 基于 Hadoop + Spark 的大规模网站相似度检测与推荐系统

## 项目简介

本项目实现了一个面向 **Linux Hadoop + Spark 集群** 的课程级 MVP，用于从网站超链接有向边数据中计算网站相似度，并为每个网站输出 Top-K 相似网站推荐结果。

项目优先面向以下运行方式：

- 集群运行：`spark-submit --master yarn`
- 本地调试：`spark-submit --master local[*]`

本项目默认使用 **PySpark DataFrame API** 实现，输入输出同时兼容：

- 本地文件系统路径，例如：`data/sample/edges_sample.txt`
- HDFS 路径，例如：`hdfs:///user/yourname/linksim/input/edges.txt`

说明：

- 运行入口应使用 `spark-submit`，而不是 `python src/main.py`
- 本地模式只用于功能调试和样例演示
- 集群模式才是项目的主要目标环境

## 相似度定义

设：

- `out(A)`：网站 `A` 的所有 referree 集合
- `out(B)`：网站 `B` 的所有 referree 集合

若 `|out(A) ∪ out(B)| > 0`，则：

`sim(A, B) = |out(A) ∩ out(B)| / |out(A) ∪ out(B)|`

否则：

`sim(A, B) = 0`

本项目采用共享 referree 的候选生成方式，避免暴力枚举所有网站对。

## 输入格式

输入是纯文本边列表，每行两个字段：

```text
src_id dst_id
```

含义：

- `src_id`：referrer
- `dst_id`：referree

示例：

```text
1 10
1 11
2 10
```

## 输出格式

项目输出根目录下最多包含三个 Spark 文本输出目录：

- `cleaned_edges`
- `pair_similarity`
- `topk_recommendations`

说明：

- `topk_recommendations` 始终输出
- `cleaned_edges` 和 `pair_similarity` 在启用 `--write-intermediate` 时输出

各目录文本格式如下。

### 1. cleaned_edges

```text
src_id dst_id
```

### 2. pair_similarity

```text
site_a site_b similarity
```

这里的 `(site_a, site_b)` 是无序唯一 pair，满足稳定顺序：

- 若两个 ID 都可解析为数字，则按数值比较
- 若无法同时按数字比较，则回退为字符串字典序

### 3. topk_recommendations

```text
site_id similar_site similarity rank
```

排序规则：

- 先按 `similarity` 降序
- 若 `similarity` 相同，则 `similar_site` 按稳定顺序升序
- 每个 `site_id` 仅保留前 `K` 个相似网站

## 算法思路

### 1. 数据清洗

- 读取原始边文件
- 去除空行
- 去除字段数不等于 2 的非法行
- 去除首尾空白
- 对 `(src_id, dst_id)` 去重

### 2. 候选网站对生成

不直接对所有网站做两两枚举，而是先从 referree 反推：

- 构建倒排视角：`dst_id -> 多个 src_id`
- 对指向同一个 `dst_id` 的多个 `src_id` 生成候选 pair
- 统计每个 pair 共享 referree 的次数，即 `intersection(A, B)`

### 3. out-degree 统计

- 对清洗后的边数据按 `src_id` 分组
- 得到每个网站的 `out_degree`

### 4. 相似度计算

对候选 pair `(A, B)`：

- `union(A, B) = outdeg(A) + outdeg(B) - intersection(A, B)`
- `similarity = intersection / union`

### 5. Top-K 推荐

- 将无序 pair `(A, B)` 扩展成双向记录：`A -> B` 和 `B -> A`
- 对每个 `site_id` 取相似度最高的前 `K` 个网站

## 系统结构

```text
LinkSim/
├── README.md
├── requirements.txt
├── data/
│   ├── sample/
│   │   └── edges_sample.txt
│   └── output_example/
│       ├── cleaned_edges_example.txt
│       ├── pair_similarity_example.txt
│       └── topk_example.txt
├── scripts/
│   ├── run_local.sh
│   └── run_hdfs.sh
└── src/
    ├── main.py
    ├── preprocess.py
    ├── similarity.py
    ├── topk.py
    └── utils.py
```

## 运行环境

项目优先面向 Linux Hadoop + Spark 集群。参考环境变量如下：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/pastorale/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/home/pastorale/spark
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

安装依赖：

```bash
python3 -m pip install -r requirements.txt
```

## 程序参数

主程序入口：`src/main.py`

支持参数：

- `--input`：输入路径，支持本地路径和 `hdfs:///` 路径
- `--output`：输出根目录，支持本地路径和 `hdfs:///` 路径
- `--topk`：每个网站保留的 Top-K 相似网站数量
- `--master`：Spark master，例如 `local[*]` 或 `yarn`
- `--write-intermediate`：启用后输出 `cleaned_edges` 和 `pair_similarity`
- `--max-referrers-per-dst`：限制单个 referree 可参与候选生成的最大 referrer 数；不设或设为 `0` 表示不限制

## 路径说明

README 中明确区分三类路径：

### 本地输入路径

```text
data/sample/edges_sample.txt
./dataset/edges.txt
```

### HDFS 输入路径

```text
hdfs:///user/yourname/linksim/input/edges_sample.txt
hdfs:///data/linksim/edges.txt
```

### 输出根目录

```text
./output/local_run
hdfs:///user/yourname/linksim/output/run_01
```

## 本地调试运行

本地模式仅用于调试。推荐命令：

```bash
spark-submit \
  --master local[*] \
  --py-files src/utils.py,src/preprocess.py,src/similarity.py,src/topk.py \
  src/main.py \
  --master local[*] \
  --input data/sample/edges_sample.txt \
  --output output/local_run \
  --topk 2 \
  --write-intermediate
```

也可以直接使用脚本：

```bash
bash scripts/run_local.sh
```

说明：

- 如果 Hadoop 配置中的 `fs.defaultFS` 指向 HDFS，裸路径如 `data/sample/edges_sample.txt` 可能被错误解析为 HDFS 路径
- 当前程序会自动把未带协议的本地路径规范化为 `file://绝对路径`
- 因此本地调试时可以直接传相对路径，也可以显式写成 `file:///absolute/path/to/edges_sample.txt`

## HDFS 上传与下载命令

以下命令统一使用 `hdfs dfs`。

上传样例数据：

```bash
hdfs dfs -mkdir -p /user/$USER/linksim/input
hdfs dfs -put -f data/sample/edges_sample.txt /user/$USER/linksim/input/edges_sample.txt
hdfs dfs -ls /user/$USER/linksim/input
```

查看输出目录：

```bash
hdfs dfs -ls /user/$USER/linksim/output
hdfs dfs -ls /user/$USER/linksim/output/run_sample
```

下载输出结果：

```bash
hdfs dfs -get hdfs:///user/$USER/linksim/output/run_sample ./output_from_hdfs
```

## YARN 集群运行

推荐以 `spark-submit` 提交：

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files src/utils.py,src/preprocess.py,src/similarity.py,src/topk.py \
  src/main.py \
  --master yarn \
  --input hdfs:///user/$USER/linksim/input/edges_sample.txt \
  --output hdfs:///user/$USER/linksim/output/run_sample \
  --topk 2 \
  --write-intermediate
```

也可以使用脚本：

```bash
bash scripts/run_hdfs.sh
```

## 样例数据

样例输入文件：`data/sample/edges_sample.txt`

该样例同时包含：

- 合法边
- 重复边
- 空行
- 非法行

清洗后的标准边表示例见：

- `data/output_example/cleaned_edges_example.txt`

## 样例结果

### cleaned_edges 示例

```text
1 10
1 11
2 10
2 11
2 12
3 11
3 12
4 12
4 13
5 13
6 99
```

### pair_similarity 示例

```text
1 2 0.6666666667
1 3 0.3333333333
2 3 0.6666666667
2 4 0.2500000000
3 4 0.3333333333
4 5 0.5000000000
```

### topk_recommendations 示例（K=2）

```text
1 2 0.6666666667 1
1 3 0.3333333333 2
2 1 0.6666666667 1
2 3 0.6666666667 2
3 2 0.6666666667 1
3 1 0.3333333333 2
4 5 0.5000000000 1
4 3 0.3333333333 2
5 4 0.5000000000 1
```

整理后的文本样例见：

- `data/output_example/cleaned_edges_example.txt`
- `data/output_example/pair_similarity_example.txt`
- `data/output_example/topk_example.txt`

## 脚本说明

- `scripts/run_local.sh`
  使用 `spark-submit --master local[*]` 在本地调试运行样例数据

- `scripts/run_hdfs.sh`
  使用 `hdfs dfs` 上传样例数据，并用 `spark-submit --master yarn` 提交到集群

## 设计说明

### 为什么不暴力枚举所有网站对

如果直接对所有网站做两两比较，网站数量较大时会产生过多无意义 pair。项目改用：

- 共享 referree 生成候选 pair

只有至少共享一个目标网站的两个网站，才进入相似度计算阶段。

### 为什么保留 `--max-referrers-per-dst`

某些 referree 可能被极大量网站指向，导致候选 pair 急剧膨胀。该参数用于后续控制：

- 若设置为正整数，则忽略 referrer 数超过阈值的 referree
- 这是一个面向大规模场景的近似化开关

默认值为 `0`，表示不限制，保持精确结果。

## 性能优化思路

当前版本优先保证能跑通、结构清晰、便于课程展示。后续可沿以下方向优化：

- 对高入度 `dst_id` 做数据倾斜治理
- 根据规模调整 `spark.sql.shuffle.partitions`
- 对复用的中间表采用 `cache/persist`
- 将文本中间结果改为 Parquet
- 增加运行统计信息与监控日志

## 后续改进方向

### 1. 数据倾斜处理

- 识别超高入度 referree
- 对热点 key 做单独处理或分桶处理

### 2. 分区优化

- 以 `dst_id` 为中心优化候选生成阶段的分区
- 控制 shuffle 分区数量，减少小文件

### 3. Cache 策略

- 对 `cleaned_edges` 和 `pair_similarity` 做按需缓存
- 避免对一次性中间表过度缓存

### 4. 近似算法扩展

- 使用 MinHash 对出链集合做压缩表示
- 使用 LSH 召回近似候选，再做精确计算

## 假设说明

- 输入文件使用空白符分隔两个字段
- 网站 ID 默认按字符串存储
- 若两个 ID 都可解析为整数，则稳定排序优先按数值比较
- 若无法同时按数值比较，则回退为字典序
- 当 `--max-referrers-per-dst` 大于 `0` 时，系统会忽略高入度 referree，对结果做受控近似
