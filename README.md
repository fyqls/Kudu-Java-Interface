# Kudu-Java-Interface
using kudu api to ease the process of creating tables and generating random data and adding data in bulk mode

Usage:

Table create:
KUDUMASTERHOST TABLENAME PRIMARYKEY COLUMNNAME TYPE COLUMNNAME TYPE

Insert Into Table:

KUDUMASTERHOST TABLENAME PATH_TO_FILE DATA_TYPE,DATA_TYPE

Transactions generation:

KUDUMASTERHOST TABLENAME PATH_TO_FILE  LAST_TRANSACTION_ID NUMBER_OF_TRANSACTION_PER_USER
