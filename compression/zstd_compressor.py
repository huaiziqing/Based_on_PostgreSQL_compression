import zstandard as zstd
import os
import logging
import time


def compress_with_zstd(input_file, output_file):
    """
    使用ZSTD流式压缩（分块读取避免内存溢出）
    
    参数:
        input_file (str): 输入文件路径
        output_file (str): 输出压缩文件路径
    """
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

        ratio = (1 - compressed_size / original_size) * 100
        logging.info(f"ZSTD压缩完成 - 原始大小: {original_size} bytes, 压缩率: {ratio:.2f}%")

        return original_size, compressed_size

    except Exception as e:
        logging.error(f"压缩失败: {input_file} -> {output_file} - {str(e)}")
        raise


def decompress_with_zstd(compressed_file, output_file):
    """
    使用ZSTD流式解压
    
    参数:
        compressed_file (str): 输入压缩文件路径
        output_file (str): 输出解压文件路径
    """
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