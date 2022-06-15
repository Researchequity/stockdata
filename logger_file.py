import logging
import os

from filepath import LOG_DIR

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def LOG_insert(file_name,text, level):
    file=os.path.join(LOG_DIR,file_name)
    print(file, "logger")
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(formatLOG)
    logger = logging.getLogger(file)
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)

    infoLog.close()
    logger.removeHandler(infoLog)

    return
