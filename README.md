# 数据压缩项目文档

## 项目概述

这是一个基于PostgreSQL的数据压缩项目，主要功能是将文本格式的数据文件（如CSV、TSV、TBL等）转换为Parquet列式存储格式，然后使用ZSTD算法进行压缩。项目还支持解压和还原原始文件格式。

## 项目结构

```
pythonProject2/
├── utils/
│   ├── __init__.py
│   ├── logging_utils.py
│   └── compression_stats.py
├── data_processing/
│   ├── __init__.py
│   └── data_reader.py
├── conversion/
│   ├── __init__.py
│   └── parquet_converter.py
├── compression/
│   ├── __init__.py
│   └── zstd_compressor.py
├── main.py
└── compression.log
```

## 模块说明

### utils 模块
- `logging_utils.py`: 配置全局日志记录系统
- `compression_stats.py`: 管理压缩统计信息，包括原始大小、压缩大小、处理文件数量和耗时统计

### data_processing 模块
- `data_reader.py`: 实现分块读取各种文本格式的数据文件，自动检测和处理不同的分隔符和编码问题

### conversion 模块
- `parquet_converter.py`: 负责在Parquet列式存储格式和文本格式之间进行转换

### compression 模块
- `zstd_compressor.py`: 使用ZSTD算法实现高效的流式压缩和解压功能

## 主要功能

1. **自动格式识别**：能够自动检测输入文本文件的格式和分隔符
2. **内存高效处理**：采用分块处理机制，避免一次性加载大文件到内存
3. **列式存储优化**：使用Parquet格式存储数据，利用列式数据库的优势提高压缩效率
4. **多线程压缩**：使用ZSTD算法的多线程功能加速压缩过程
5. **完整的数据转换**：支持Parquet格式与原始文本格式之间的双向转换

## 运行流程

1. 从`data/`目录读取原始数据文件
2. 将数据分块转换为Parquet列式存储格式
3. 使用ZSTD算法对Parquet文件进行压缩
4. 解压压缩文件并还原为原始格式
5. 生成详细的压缩性能报告

## 依赖库
- pandas: 用于数据操作和分块处理
- pyarrow/parquet: 用于Parquet格式的读写
- zstandard: 用于ZSTD压缩和解压
- logging: 用于日志记录

## 日志文件
所有运行日志都会被记录到`compression.log`文件中，包含INFO、WARNING、ERROR等级别的信息。

## 统计报告
程序结束时会输出一个汇总统计报告，显示处理的文件总数、原始数据总大小、压缩后数据总大小、平均压缩率以及总的压缩和解压耗时。

## 使用方法
1. 确保安装了所有依赖库
2. 将需要处理的数据文件放入`data/`目录
3. 运行`main.py`
4. 处理结果会保存在`compress/`和`decompress/`目录中

## 注意事项
1. 确保有足够的磁盘空间处理大文件
2. 根据系统资源情况调整分块大小
3. ZSTD压缩级别可以在配置中调整以平衡压缩速度和压缩率