from enum import Enum
#from threading import Thread


operation_names = ("ls","fetch","upload","read","quit")
OPERATIONS = Enum("OPERATIONS",operation_names)

NameNode_path = "./dfs/NameNode"

DataNode_path = "./dfs/DataNode"


infofile = NameNode_path + "/info.pkl"

NUM_OF_DATASERVERS = 4

NUM_OF_REPLICAS = 3

CHUNK_SIZE = 2 * 1024 * 1024