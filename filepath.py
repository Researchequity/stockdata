import os

# Getting path of Project Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# setting path for folders
RAW_DIR = os.path.join(BASE_DIR, 'raw')
LOG_DIR = os.path.join(BASE_DIR, 'log')
PROCESSED_DIR = os.path.join(BASE_DIR, 'processed')
METADATA_DIR = os.path.join(BASE_DIR, 'metadata')
SS_DIR = os.path.join(BASE_DIR, 'screenshot')

# setting path for cloud
SHARED_DIR = r'\\192.168.41.190\shared'
REPORT_DIR = r'\\192.168.41.190\report'


