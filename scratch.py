import logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# log_file_handler = logging.FileHandler("log.txt")
# logger.addHandler(log_file_handler)
# logger.debug("A little detail")
# logger.warning("Boo!")


logger1 = logging.getLogger()
logger1.setLevel(logging.INFO)
log_file_hander1 = logging.FileHandler("log1_output.txt")

logger2 = logging.getLogger()
logger2.setLevel(logging.INFO)
log_file_hander2 = logging.FileHandler("log2_output.txt")

logger2.addHandler(log_file_hander2)
logger2.info("this is logger 2i, stm 1")
logger2.removeHandler(log_file_hander2)

logger1.addHandler(log_file_hander1)
logger1.info("this is logger 1")
logger1.removeHandler(log_file_hander1)

logger2.addHandler(log_file_hander2)
logger2.info("this is for logger 2, stm 2")
logger2.removeHandler(log_file_hander2)

