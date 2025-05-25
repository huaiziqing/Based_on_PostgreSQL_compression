import pandas as pd
import logging
from collections import Counter


def read_file_data_in_chunks(file_path, file_type):
    """
    分块读取结构化文件数据并生成迭代器
    
    参数:
        file_path (str): 输入文件路径
        file_type (str): 文件类型（tbl/csv/txt）
    
    返回:
        Iterator[pd.DataFrame]: 数据块迭代器
    
    异常:
        ValueError: 当文件类型不支持或文件内容异常时抛出
    """
    if file_type == "tbl":
        sep = "|"
        quotechar = '"'  # 新增引号处理
        escapechar = '\\'
    elif file_type == "csv":
        sep = ","
        quotechar = '"'
        escapechar = '\\'
    elif file_type == "txt":
        sep = "\t"
        quotechar = '"'
        escapechar = '\\'
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    try:
        # 增强型列数检测
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            sep = '|' if file_path == 'tbl' else ','
            expected_columns = len(first_line.split(sep))

            # 读取足够多的行确保检测准确
            preview_lines = [f.readline() for _ in range(20) if f.readline()]

            # 动态检测最佳分隔符（备用方案）
            if not preview_lines:
                raise ValueError("空文件")

            # 排除空行和注释行
            valid_lines = [line.strip() for line in preview_lines if line.strip() and not line.startswith("#")]
            if not valid_lines:
                raise ValueError("没有有效数据行")
            
            # 列数一致性检查
            column_counts = [len(line.split(sep)) for line in valid_lines]
            if len(set(column_counts)) > 1:
                logging.warning("检测到动态列数，尝试修复...")
                
            # 尝试寻找更可靠的分隔符
            possible_seps = ['|', ',', '\t']
            sep_counter = Counter()
            for line in valid_lines:
                for s in possible_seps:
                    sep_counter[s] += line.count(s)
            detected_sep = sep_counter.most_common(1)[0][0]
            if detected_sep != sep:
                logging.info(f"自动切换分隔符为: {repr(detected_sep)}")
                sep = detected_sep
                column_counts = [len(line.split(sep)) for line in valid_lines]
                num_columns = max(set(column_counts), key=column_counts.count)
                logging.info(f"检测到稳定列数: {num_columns}")
                
            # 动态列名生成
            column_names = [f"col_{i}" for i in range(num_columns)]
            
        chunk_iterator = pd.read_csv(
            file_path,
            sep=sep,
            header=None,
            names=column_names,
            dtype='category',  # 整体使用category类型
            quotechar=quotechar,
            escapechar=escapechar,
            chunksize=50000,  # 每次读取50000行
            engine='python',  # 使用Python引擎增强兼容性
            encoding_errors='replace'
        )
        return chunk_iterator
    except Exception as e:
        logging.error(f"解析失败：{file_path} - {str(e)}")
        raise