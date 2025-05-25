from collections import defaultdict


def init_compression_stats():
    """
    初始化压缩统计信息字典
    
    返回:
        dict: 包含以下键的字典
            - total_original: 原始数据总大小（字节）
            - total_compressed: 压缩后数据总大小（字节）
            - file_count: 处理文件数量
            - total_compress_time: 总压缩时间（秒）
            - total_decompress_time: 总解压时间（秒）
    """
    return {
        'total_original': 0,
        'total_compressed': 0,
        'file_count': 0,
        'total_compress_time': 0,
        'total_decompress_time': 0,
    }


def update_compression_stats(stats, original_size, compressed_size):
    """
    更新压缩统计信息
    
    参数:
        stats (dict): 压缩统计信息字典
        original_size (int): 原始文件大小（字节）
        compressed_size (int): 压缩后文件大小（字节）
    """
    stats['total_original'] += original_size
    stats['total_compressed'] += compressed_size
    stats['file_count'] += 1


def print_summary_stats(stats):
    """
    打印汇总统计信息
    
    参数:
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