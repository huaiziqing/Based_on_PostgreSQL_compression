import logging


def setup_logging():
    """
    配置全局日志记录器
    
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