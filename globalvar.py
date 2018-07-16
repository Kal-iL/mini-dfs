from threading import Event
from globalconsts import *

# variables below are used to communicate between all servers

OPERATION = None

# main awake namenode to work
main2name_event = Event()
name2main_event = Event()
# dataevents : namenode awake datanode to work
name2data_events = [Event() for i in range(NUM_OF_DATASERVERS)]
data2name_events = [Event() for i in range(NUM_OF_DATASERVERS)]
# mainevents: datanodes notify main work is done
# mainevents = [Event() for i in range(NUM_OF_DATASERVERS)] 

global_flag = False

#These variables are defined to pass information from client to NameNode


# upload_config={
#     "file_path":None,
#     "block_server_map" : {}
# }
# fetch_config = {
#     "file_id":None,
#     "save_path":None,
#     "blocks_to_be_fetch":{},
#     "fetch_results":{}
# }
# read_config ={
#     "file_id":None,
#     "offset": None,
#     "count": None,
#     "start":None,
#     "block":None,
#     "server":None
# }

# upload_para = {
#     "file_path":None,
# }
# upload parameters
upload_file = None

#fetch parameters

fetch_file_id = None
fetch_save_file = None
#fetch_save_name = None
# fetch_para = {
#     "file_id":None,
#     "save_path":None,
# }

#read_parameters

read_file_id = None
read_offset = None
read_count = None


# read_para ={
#     "file_id": None,
#     "offset": None,
#     "count" : None
#}

# These variables are defined to pass information from  NameNode and DataNodes

# upload

upload_server_block_map = None

# upload_config = {
#     "file_path":None,
#     "block_server_map":None
# }

# fetch 

fetch_server_block_map = {}
#fetch_server_block_map = {
#  1:"1-part-2",
#  2:"2-part-2"
# }
fetch_id_block_map = {}
# fetch_config ={
#     "server_block_map":{
#         1:None,
#         2:None,
#         3:None,
#         4:None
#     }
# }

# read 

NameNode_Flag = None
NameNone_error_messages = None
read_server_block_map = {}
read_block_config = {}

# read_config = {
#     "server_block_map":{},
#     #block_
#     "block_content_map":{},
# }


#These variables are defined to transfer results from DataNodes to NameNode

ls_results = []
read_results = {}
fetch_results = {}

DataNode_Flag = None
DataNone_error_messages = None 
