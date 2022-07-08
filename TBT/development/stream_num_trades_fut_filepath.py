import os
BASE_PATH="//home/workspace/development/python_ankit"
AGGREGATE_PATH = '//home/workspace/aggregate/tradewatch_fut'
DUMPER_FILE_DIRECTORY="//home/workspace/dumper"
TOKEN_SECURITY_FILE=os.path.join(BASE_PATH,'token_contract.csv')
NORMEN_TRD_FILE=os.path.join(BASE_PATH,"Average_fut.csv")

TRADE_WATCH=os.path.join(AGGREGATE_PATH)

SKIP_ROWS_NUM_DIRECTORY=os.path.join(TRADE_WATCH,"metadata")
OPENING_DATA=os.path.join(TRADE_WATCH)
LAST_MIN_DATA=os.path.join(TRADE_WATCH,'metadata')
LOG_FILE_DIRECTORY=os.path.join(TRADE_WATCH,"metadata")
