import pyarrow as pa
import pyarrow.parquet as pq
import logging


def convert_to_columnar_in_chunks(chunk_iterator, output_file, file_type):
    """
    将数据块迭代器转换为Parquet列式存储格式
    
    参数:
        chunk_iterator (Iterator[pd.DataFrame]): 输入数据块迭代器
        output_file (str): 输出Parquet文件路径
        file_type (str): 原始文件类型（用于元数据）
    
    异常:
        Exception: 转换过程中出现错误时抛出
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
        raise


def convert_parquet_to_text_in_chunks(parquet_file_path, text_file_path, file_type):
    """
    将Parquet文件分块转换为文本格式
    
    参数:
        parquet_file_path (str): 输入Parquet文件路径
        text_file_path (str): 输出文本文件路径
        file_type (str): 目标文件类型（tbl/csv/txt）
    
    异常:
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