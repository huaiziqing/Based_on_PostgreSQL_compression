import zstandard as zstd
import os
import logging
import time

def compress_with_zstd(input_file, output_file):
    """
    使用ZSTD流式压缩（分块读取避免内存溢出）
    Args:
        input_file (str): 输入文件路径
        output_file (str): 输出文件路径
    Returns:
        tuple: (原始大小, 压缩后大小)
    Raises:
        Exception: 压缩过程中出现错误时抛出
    """
    try:
        # 配置多线程压缩器
        cctx = zstd.ZstdCompressor(level=18, threads=os.cpu_count())
        chunk_size = 4096 * 1024  # 4MB块大小（降低内存使用）

        start_time = time.time()
        
        with open(input_file, "rb") as f_in, open(output_file, "wb") as f_out:
            # 创建压缩流写入器
            with cctx.stream_writer(f_out) as compressor:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    compressor.write(chunk)

        duration = time.time() - start_time

        # 统计压缩后大小
        original_size = os.path.getsize(input_file)
        compressed_size = os.path.getsize(output_file)
        
        ratio = (1 - compressed_size / original_size) * 100
        logging.info(f"ZSTD压缩完成 - 原始大小: {original_size} bytes, 压缩率: {ratio:.2f}%")
        
        return original_size, compressed_size
        
    except Exception as e:
        logging.error(f"压缩失败: {input_file} -> {output_file} - {str(e)}")
        raise

def decompress_with_zstd(compressed_file, output_file):
    """
    使用ZSTD流式解压
    Args:
        compressed_file (str): 压缩文件路径
        output_file (str): 解压后的输出文件路径
    Raises:
        Exception: 解压过程中出现错误时抛出
    """
    try:
        dctx = zstd.ZstdDecompressor()
        chunk_size = 4096 * 1024  # 4MB块大小

        start_time = time.time()
        
        with open(compressed_file, "rb") as f_in, open(output_file, "wb") as f_out:
            # 创建解压流读取器
            with dctx.stream_reader(f_in) as reader:
                while True:
                    chunk = reader.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    
        duration = time.time() - start_time
                    
        logging.info(f"解压完成: {compressed_file} -> {output_file}")
        
    except Exception as e:
        logging.error(f"解压失败: {compressed_file} - {str(e)}")
        raise
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import logging

def convert_to_columnar_in_chunks(chunk_iterator, output_file, file_type):
    """
    将数据块迭代器转换为Parquet列式存储格式
    Args:
        chunk_iterator (Iterator[pd.DataFrame]): 输入数据块迭代器
        output_file (str): 输出Parquet文件路径
        file_type (str): 原始文件类型用于元数据
    Raises:
        Exception: 转换过程中出现错误时抛出
    """
    try:
        first_chunk = next(chunk_iterator)
        schema = pa.Schema.from_pandas(first_chunk, preserve_index=False)
        
        # 添加原始文件类型元数据
        schema = schema.with_metadata({b'original_extension': file_type.encode()})
        
        # 对字符串列启用压缩
        for field in schema:
            if pa.types.is_string(field.type):
                new_field = field.with_metadata({b'compression': b'ZSTD'})
                schema = schema.set(schema.get_field_index(field.name), new_field)

        # 重命名列以保证一致性
        first_chunk.columns = [f"col_{i}" for i in range(first_chunk.shape[1])]
        schema = pa.Schema.from_pandas(first_chunk, preserve_index=False)
        table = pa.Table.from_pandas(first_chunk)

        # 创建Parquet写入器
        parquet_writer = pq.ParquetWriter(
            output_file,
            schema,
            compression='NONE',
            use_dictionary=True,
            write_statistics=True,
            flavor='spark'
        )
        
        # 写入第一个数据块
        parquet_writer.write_table(table)
        logging.info(f"Parquet schema初始化完成: {schema}")

        # 处理剩余的数据块
        for chunk in chunk_iterator:
            chunk.columns = [f"col_{i}" for i in range(chunk.shape[1])]
            table = pa.Table.from_pandas(chunk)
            parquet_writer.write_table(table)
            logging.debug(f"写入分块数据，大小: {chunk.shape[0]}行")

        # 完成写入
        parquet_writer.close()
        logging.info(f"列式存储文件生成: {output_file}")

    except Exception as e:
        logging.error(f"列式转换失败: {str(e)}")
        raise

def convert_parquet_to_text_in_chunks(parquet_file_path, text_file_path, file_type):
    """
    将Parquet文件分块转换为文本格式
    Args:
        parquet_file_path (str): 输入Parquet文件路径
        text_file_path (str): 输出文本文件路径
        file_type (str): 目标文件类型（tbl/csv/txt）
    Raises:
        ValueError: 不支持的文件类型时抛出
    """
    if file_type == "tbl":
        sep = "|"
    elif file_type == "csv":
        sep = ","
    elif file_type == "txt":
        sep = "\t"
    else:
        raise ValueError(f"不支持的文件类型: {file_type}")
        
    try:
        # 使用 newline='' 让系统自动处理换行符
        with pq.ParquetFile(parquet_file_path) as parquet_file:
            with open(text_file_path, "w", encoding="utf-8", newline='') as f:
                is_first_chunk = True
                for batch in parquet_file.iter_batches(batch_size=50000):
                    df = batch.to_pandas()
                    
                    # 仅在非首行时写入时添加换行符
                    df_string = df.to_csv(
                        sep=sep,
                        header=False,
                        index=False,
                        lineterminator='\n'  # 强制使用 \n 换行
                    )
                    
                    if not is_first_chunk:
                        f.write('\n')  # 手动添加分块间的换行
                    f.write(df_string.strip('\n'))  # 移除末尾自带的换行符
                    is_first_chunk = False
                    
        logging.info(f"成功转换 Parquet 到文本: {text_file_path}")
        
    except Exception as e:
        logging.error(f"Parquet 转换失败: {parquet_file_path} -> {text_file_path} - {str(e)}")
        raise
import pandas as pd
import logging
from io import StringIO

def read_file_data_in_chunks(file_path, file_type):
    """
    分块读取结构化文件数据并生成迭代器
    Args:
        file_path (str): 输入文件路径
        file_type (str): 文件类型（tbl/csv/txt）
    Returns:
        Iterator[pd.DataFrame]: 数据块迭代器
    Raises:
        ValueError: 当文件类型不支持或文件内容异常时抛出
    """
    if file_type == "tbl":
        sep = "|"
        quotechar = '"'
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
            sep = '|' if file_path.endswith('tbl') else ','  # 更准确的分隔符判断
            expected_columns = len(first_line.split(sep))

            # 读取足够多的行确保检测准确
            preview_lines = [f.readline() for _ in range(20) if f.readline()]

            # 排除空行和注释行
            valid_lines = [line.strip() for line in preview_lines if line.strip() and not line.startswith("#")]
            if not valid_lines:
                raise ValueError("没有有效数据行")
                
            # 列数一致性检查
            column_counts = [len(line.split(sep)) for line in valid_lines]
            
            if len(set(column_counts)) > 1:
                logging.warning("检测到动态列数，尝试修复...")
                
                # 尝试寻找更可靠的分隔符
                from collections import Counter
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
                    
            num_columns = expected_columns
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
            chunksize=50000,  # 使用常量
            engine='python',  # 使用Python引擎增强兼容性
            encoding_errors='replace'
        )
        
        return chunk_iterator
        
    except Exception as e:
        logging.error(f"解析失败：{file_path} - {str(e)}")
        raise
def init_compression_stats():
    """
    初始化压缩统计信息
    Returns:
        dict: 包含统计信息的字典
    """
    return {
        'total_original': 0,
        'total_compressed': 0,
        'file_count': 0,
        'total_compress_time': 0,
        'total_decompress_time': 0
    }

def update_compression_stats(stats, original_size, compressed_size):
    """
    更新压缩统计信息
    Args:
        stats (dict): 统计信息字典
        original_size (int): 原始文件大小
        compressed_size (int): 压缩后文件大小
    """
    stats['total_original'] += original_size
    stats['total_compressed'] += compressed_size
    stats['file_count'] += 1

def print_summary_stats(stats):
    """
    打印汇总统计信息
    Args:
        stats (dict): 包含统计信息的字典
    """
    if stats['file_count'] == 0:
        logging.warning("没有压缩文件可供统计")
        return

    total_ratio = (1 - stats['total_compressed'] / stats['total_original']) * 100
    print("\n====== 压缩汇总统计 ======")
    print(f"处理文件总数: {stats['file_count']}")
    print(f"总原始大小: {stats['total_original'] / 1024 / 1024:.2f} MB")
    print(f"总压缩大小: {stats['total_compressed'] / 1024 / 1024:.2f} MB")
    print(f"平均压缩率: {total_ratio:.2f}%")
    print(f"总压缩耗时: {stats['total_compress_time']:.2f}秒")
    print(f"总解压耗时: {stats['total_decompress_time']:.2f}秒")
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_file='compression.log', level=logging.INFO):
    """
    配置日志系统
    Args:
        log_file: 日志文件路径
        level: 日志级别
    """
    # 创建日志格式器
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 创建文件处理器（带轮转）
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    
    # 获取根日志记录器并配置
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd
import logging
from io import StringIO
import time

# 定义文件路径
data_dir = "data"
compress_dir = "compress"
decompress_dir = "decompress"

"""
该字典用于记录压缩流程的全局统计信息
包括原始数据总大小、压缩后数据总大小和处理文件数量
最终生成汇总报告以评估压缩性能
通过 compress_with_zstd 函数在每次压缩完成后更新字典
"""

compression_stats = {
    'total_original': 0,
    'total_compressed': 0,
    'file_count': 0,
    'total_compress_time': 0,  # 新增总压缩时间（秒）
    'total_decompress_time': 0,  # 新增总解压时间（秒）
}

# 配置日志
"""
DEBUG（最低级别，用于详细信息）
INFO（用于一般信息）
WARNING（用于警告信息）
ERROR（用于错误信息）
CRITICAL（最高级别，用于严重错误）

%(asctime)s：记录日志的时间
%(levelname)s：日志级别名称（如INFO、WARNING等）
%(name)s：日志记录器的名称
%(message)s：日志消息本身
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler("compression.log")  # 写入日志文件
    ]
)

# 确保目录存在
os.makedirs(compress_dir, exist_ok=True)
os.makedirs(decompress_dir, exist_ok=True)

# 分块大小
chunk_size = 50000  # 每次读取50000行

def read_file_data_in_chunks(file_path, file_type):
    """
           分块读取结构化文件数据并生成迭代器
           file_path (str): 输入文件路径
           file_type (str): 文件类型（tbl/csv/txt）
           sample_size (int): 用于分析的样本行数
           Iterator[pd.DataFrame]: 数据块迭代器
           ValueError: 当文件类型不支持或文件内容异常时抛出
           Pandas的分块读取机制结合动态错误处理，提升文件解析鲁棒性
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
            first_line=f.readline().strip()
            sep='|' if file_path=='tbl' else ','
            expected_columns=len(first_line.split(sep))

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
            from collections import Counter
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
        chunk_iterator=pd.read_csv(
            file_path,
            sep=sep,
            header=None,
            names=column_names,
            dtype='category',  # 整体使用category类型
            quotechar=quotechar,
            escapechar=escapechar,
            chunksize=chunk_size,
            engine='python',  # 使用Python引擎增强兼容性
            encoding_errors='replace'
        )
        return chunk_iterator
    except Exception as e:
        logging.error(f"解析失败：{file_path} - {str(e)}")
        raise


def convert_to_columnar_in_chunks(chunk_iterator, output_file,file_type):
    """
       将数据块迭代器转换为Parquet列式存储格式
           chunk_iterator (Iterator[pd.DataFrame]): 输入数据块迭代器
           output_file (str): 输出Parquet文件路径
           Exception: 转换过程中出现错误时抛出
           Parquet的列式存储通过字典编码优化存储效率
    """
    try:
        first_chunk = next(chunk_iterator)
        schema = pa.Schema.from_pandas(first_chunk, preserve_index=False)
        schema = schema.with_metadata({b'original_extension': file_type.encode()})
        for field in schema:
            if pa.types.is_string(field.type):
                # 对长文本列启用压缩
                new_field = field.with_metadata({b'compression': b'ZSTD'})
                schema = schema.set(schema.get_field_index(field.name), new_field)

        first_chunk.columns = [f"col_{i}" for i in range(first_chunk.shape[1])]
        schema = pa.Schema.from_pandas(first_chunk, preserve_index=False)
        table = pa.Table.from_pandas(first_chunk)

        parquet_writer = pq.ParquetWriter(
            output_file,
            schema,
            compression='NONE',
            use_dictionary=True,
            write_statistics=True,
            flavor='spark'
        )
        parquet_writer.write_table(table)
        logging.info(f"Parquet schema初始化完成: {schema}")

        for chunk in chunk_iterator:
            chunk.columns = [f"col_{i}" for i in range(chunk.shape[1])]
            table = pa.Table.from_pandas(chunk)
            parquet_writer.write_table(table)
            logging.debug(f"写入分块数据，大小: {chunk.shape[0]}行")

        parquet_writer.close()
        logging.info(f"列式存储文件生成: {output_file}")

    except Exception as e:
        logging.error(f"列式转换失败: {str(e)}")


def compress_with_zstd(input_file, output_file):
    """使用ZSTD流式压缩（分块读取避免内存溢出）"""
    try:
        # 配置多线程压缩器
        cctx = zstd.ZstdCompressor(level=18, threads=os.cpu_count())
        chunk_size = 4096000 * 1024  # 1GB块大小

        with open(input_file, "rb") as f_in, open(output_file, "wb") as f_out:
            # 创建压缩流写入器
            with cctx.stream_writer(f_out) as compressor:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    compressor.write(chunk)

        # 统计压缩后大小
        original_size = os.path.getsize(input_file)
        compressed_size = os.path.getsize(output_file)

        # 更新统计信息
        compression_stats['total_original'] += original_size
        compression_stats['total_compressed'] += compressed_size
        compression_stats['file_count'] += 1

        ratio = (1 - compressed_size / original_size) * 100
        logging.info(f"ZSTD压缩完成 - 原始大小: {original_size} bytes, 压缩率: {ratio:.2f}%")

    except Exception as e:
        logging.error(f"压缩失败: {input_file} -> {output_file} - {str(e)}")
        raise


def decompress_with_zstd(compressed_file, output_file):
    """使用ZSTD流式解压"""
    try:
        dctx = zstd.ZstdDecompressor()
        chunk_size = 4096 * 1024  # 1MB块大小

        with open(compressed_file, "rb") as f_in, open(output_file, "wb") as f_out:
            # 创建解压流读取器
            with dctx.stream_reader(f_in) as reader:
                while True:
                    chunk = reader.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)

        logging.info(f"解压完成: {compressed_file} -> {output_file}")

    except Exception as e:
        logging.error(f"解压失败: {compressed_file} - {str(e)}")
        raise

def convert_parquet_to_text_in_chunks(parquet_file_path, text_file_path, file_type):
    """
        将Parquet文件分块转换为文本格式
        Args:
            parquet_file_path (str): 输入Parquet文件路径
            text_file_path (str): 输出文本文件路径
            file_type (str): 目标文件类型（tbl/csv/txt）
        Raises:
            ValueError: 不支持的文件类型时抛出
    """
    if file_type == "tbl":
        sep = "|"
    elif file_type == "csv":
        sep = ","
    elif file_type == "txt":
        sep = "\t"
    else:
        raise ValueError(f"不支持的文件类型: {file_type}")
    try:
        # 使用 newline='' 让系统自动处理换行符
        with pq.ParquetFile(parquet_file_path) as parquet_file:
            with open(text_file_path, "w", encoding="utf-8", newline='') as f:  # 关键修改
                is_first_chunk = True
                for batch in parquet_file.iter_batches(batch_size=chunk_size):
                    df = batch.to_pandas()
                    # 仅在非首行时写入时添加换行符
                    df_string = df.to_csv(
                        sep=sep,
                        header=False,
                        index=False,
                        lineterminator='\n'  # 强制使用 \n 换行
                    )
                    if not is_first_chunk:
                        f.write('\n')  # 手动添加分块间的换行
                    f.write(df_string.strip('\n'))  # 移除末尾自带的换行符
                    is_first_chunk = False
        logging.info(f"成功转换 Parquet 到文本: {text_file_path}")
    except Exception as e:
        logging.error(f"Parquet 转换失败: {parquet_file_path} -> {text_file_path} - {str(e)}")
        raise


def print_summary_stats():
    """打印汇总统计信息"""
    if compression_stats['file_count'] == 0:
        logging.warning("没有压缩文件可供统计")
        return

    total_ratio = (1 - compression_stats['total_compressed'] / compression_stats['total_original']) * 100
    print("\n====== 压缩汇总统计 ======")
    print(f"处理文件总数: {compression_stats['file_count']}")
    print(f"总原始大小: {compression_stats['total_original'] / 1024 / 1024:.2f} MB")
    print(f"总压缩大小: {compression_stats['total_compressed'] / 1024 / 1024:.2f} MB")
    print(f"平均压缩率: {total_ratio:.2f}%")
    print(f"总压缩耗时: {compression_stats['total_compress_time']:.2f}秒")
    print(f"总解压耗时: {compression_stats['total_decompress_time']:.2f}秒")
def compressmain():
    """
        主流程控制函数，执行完整的压缩和解压流程：
        1. 遍历data目录下的数据文件
        2. 分块转换为Parquet格式
        3. 使用ZSTD压缩
        4. 解压并还原为原始格式
    """
    logging.info("====== 开始压缩流程 ======")
    total_start = time.time()
    for file_name in os.listdir(data_dir):
        if file_name.endswith((".tbl", ".csv", ".txt")):
            try:
                file_start = time.time()
                file_path = os.path.join(data_dir, file_name)
                file_type = file_name.split(".")[-1]
                base_name = os.path.splitext(file_name)[0]
                logging.info(f"正在处理文件: {file_name}")

                chunk_iterator = read_file_data_in_chunks(file_path, file_type)

                temp_parquet = os.path.join(compress_dir, f"{base_name}.parquet")
                convert_to_columnar_in_chunks(chunk_iterator, temp_parquet, file_type)  # 传入file_type

                compressed_file = os.path.join(compress_dir, f"{base_name}.zstd")
                compress_with_zstd(temp_parquet, compressed_file)
                os.remove(temp_parquet)

                logging.info(f"压缩完成: {compressed_file}")

                file_time = time.time() - file_start
                compression_stats['total_compress_time'] += file_time
                logging.info(f"文件 {file_name} 压缩耗时: {file_time:.2f}秒")

            except Exception as e:
                logging.error(f"文件处理失败: {file_name} - {str(e)}")
                continue
    total_time = time.time() - total_start
    compression_stats['total_compress_time'] = total_time  # 可选：覆盖为精确总时间
    logging.info(f"压缩流程总耗时: {total_time:.2f}秒")

def decompressmain():
    logging.info("====== 开始解压流程 ======")
    for compressed_file_name in os.listdir(compress_dir):
        if compressed_file_name.endswith(".zstd"):
            try:
                compressed_file_path = os.path.join(compress_dir, compressed_file_name)
                base_name = os.path.splitext(compressed_file_name)[0]
                original_name_without_ext = os.path.splitext(base_name)[0]  # "customer"
                original_ext = os.path.splitext(base_name)[1][1:]  # "tbl"

                decompressed_parquet = os.path.join(compress_dir, f"{original_name_without_ext}.parquet")

                # 解压到 Parquet
                decompress_with_zstd(compressed_file_path, decompressed_parquet)

                # 从 Parquet 元数据读取原始扩展名
                with pq.ParquetFile(decompressed_parquet) as parquet_file:  # 使用 with 确保关闭
                    metadata = parquet_file.schema_arrow.metadata
                    original_ext = metadata.get(b'original_extension', b'tbl').decode()

                # 生成最终输出路径
                output_file_name = f"{original_name_without_ext}.{original_ext}"
                output_path = os.path.join(decompress_dir, output_file_name)

                # 转换 Parquet 到文本
                convert_parquet_to_text_in_chunks(decompressed_parquet, output_path, original_ext)

                # 确保删除前文件已关闭
                os.remove(decompressed_parquet)
                logging.info(f"解压完成: {output_path}")
            except Exception as e:
                logging.error(f"解压失败 {compressed_file_name}: {str(e)}", exc_info=True)


if __name__ == "__main__":
    # 初始化日志
    setup_logging()
    
    # 初始化统计信息
    compression_stats = init_compression_stats()
    
    # 执行压缩流程
    compressmain()
    
    # 执行解压流程
    decompressmain()
    
    # 打印汇总统计
    print_summary_stats(compression_stats)
