from threading import Event
from globalconsts import *



OPERATION = None

# main awake namenode to work
main2name_event = Event()
name2main_event = Event()
# dataevents : namenode awake datanode to work
name2data_events = [Event() for i in range(NUM_OF_DATASERVERS)]
data2name_events = [Event() for i in range(NUM_OF_DATASERVERS)]


global_flag = False


upload_file = None

#fetch parameters

fetch_file_id = None
fetch_save_file = None


read_file_id = None
read_offset = None
read_count = None




upload_server_block_map = None



fetch_server_block_map = {}

fetch_id_block_map = {}


NameNode_Flag = None
NameNone_error_messages = None
read_server_block_map = {}
read_block_config = {}


ls_results = []
read_results = {}
fetch_results = {}

DataNode_Flag = None
DataNone_error_messages = None 
