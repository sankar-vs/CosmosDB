'''
@Author: Sankar
@Date: 2021-05-06 07:51:25
@Last Modified by: Sankar
@Last Modified time: 2021-05-06 08:38:09
@Title : LOGGER File
'''
import logging

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create handlers
f_handler = logging.FileHandler('loggerfile.log')
f_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(f_handler)