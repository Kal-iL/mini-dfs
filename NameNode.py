import threading
import pickle
import math
import random
import os
import globalvar

from globalconsts import *


infofile = globalvar.infofile



class NameNode(threading.Thread):
    #'''NameNode'''
    def __init__(self, name):

        super(NameNode, self).__init__(name=name)
        self.file_info = []
        self.last_file_id = -1
        self.id_block_map={}
        self.last_server_id = 0
        self.block_server_map = {}
        self.load_info()


    def load_info(self):
        if os.path.isfile(infofile):
            with open(infofile,"rb") as f:
                data = pickle.load(f)
                self.file_info = data["file_info"]
                self.id_block_map = data["id_block_map"]
                self.last_server_id = data["last_server_id"]
                self.block_server_map = data["block_server_map"]
                self.last_file_id = data["last_file_id"]
        else:
            pass

    def store_info(self):
        data = {}
        data["file_info"] = self.file_info
        data["id_block_map"] = self.id_block_map
        data["last_server_id"] = self.last_server_id
        data["block_server_map"] = self.block_server_map
        data["last_file_id"] = self.last_file_id
        with open(infofile,"wb") as f:
            pickle.dump(data,f)

    def assign_ls(self):
        globalvar.flag = True
        globalvar.ls_results = self.file_info
        #for mainevents in globalvar.mainevents:
        globalvar.name2main_event.set()

    def assign_read(self):
        globalvar.flag = True

        file_id = globalvar.read_file_id
        offset = globalvar.read_offset
        count = globalvar.read_count

        if file_id < 0 or file_id > self.last_file_id:
            print("No such file")
            globalvar.flag = False
            return

        block_id_start = int(math.floor(offset / CHUNK_SIZE))
        block_id_end = int(math.floor((offset + count) /CHUNK_SIZE ))
        
        block_to_be_read = ["%d-part-%d"%(file_id,block_id) for block_id in range(block_id_start,block_id_end + 1)]
        globalvar.read_block_config.clear()


        for i in range(block_id_start,block_id_end + 1):
            if i == block_id_start:
                start = offset  % CHUNK_SIZE
                size =  min(CHUNK_SIZE - start,count)

            elif i == block_id_end :
                start  = 0
                size = (offset + count) % CHUNK_SIZE

            else:
                start = 0
                size = CHUNK_SIZE 
            
            globalvar.read_block_config["%d-part-%d"%(file_id,i)] = (start,size)
        
        globalvar.read_server_block_map.clear()

        for block in block_to_be_read:
            server_id = random.choice(self.block_server_map[block])
            if server_id not in globalvar.read_server_block_map:
                globalvar.read_server_block_map[server_id] = []
            globalvar.read_server_block_map[server_id].append(block)

        for event in globalvar.name2data_events:
            event.set()

        for event in globalvar.data2name_events:
            event.wait()

        for event in globalvar.data2name_events:
            event.clear()
        
        globalvar.name2main_event.set()
        
    def assign_fetch(self):
        file_id = globalvar.fetch_file_id
        file_path = globalvar.fetch_save_file
        if file_id > self.last_file_id or file_id < 0:
            print("No such file!")
            globalvar.flag = false
        else:
            globalvar.fetch_server_block_map.clear()
            
            for block in self.id_block_map[file_id]:
                server = random.choice(self.block_server_map[block])
                if server not in globalvar.fetch_server_block_map:
                    globalvar.fetch_server_block_map[server] = []
                globalvar.fetch_server_block_map[server].append(block)
        for event in globalvar.name2data_events:
            event.set()


        for event in globalvar.name2data_events:
            event.set()

        for event in globalvar.data2name_events:
            event.wait()

        for event in globalvar.data2name_events:
            event.clear()

        globalvar.name2main_event.set()

    def assign_upload(self):
        filepath = globalvar.upload_file
        size = os.path.getsize(filepath)
        name = os.path.split(filepath)[1]
        
        last_file_id = self.last_file_id + 1
        num_of_blocks = int(math.ceil(float(size) / CHUNK_SIZE))
        
        id_block_map = []
        for i in range(num_of_blocks):
            #self.id_block_map[self.last_file_id].append("%d-part-%d"%(self.last_file_id,i))
            id_block_map.append("%d-part-%d"%(last_file_id,i))
    
        globalvar.upload_server_block_map = {}

        # block_server_map记录每个块存在哪个服务器上
        server_id = self.last_server_id 
        block_server_map = {}
        for count,block in enumerate(id_block_map):
            block_server_map[block] = []
            for i in range(NUM_OF_REPLICAS):
                server_id = (server_id + 1) % NUM_OF_DATASERVERS
                block_server_map[block].append(server_id)
                #server_block_map 记录datanode server需要存哪些block

                if server_id not in globalvar.upload_server_block_map:
                    globalvar.upload_server_block_map[server_id] = [] 
                file_size = CHUNK_SIZE if count != num_of_blocks - 1 else size -  CHUNK_SIZE * count
                offset = CHUNK_SIZE * count
                globalvar.upload_server_block_map[server_id].append((last_file_id,offset,file_size,count))        
        
        

        for event in globalvar.name2data_events:
            event.set()
        for event in globalvar.data2name_events:
            event.wait()
        for event in globalvar.data2name_events:
            event.clear()

        #if globalvar.DataNode_Flag:
        self.last_server_id = server_id
        self.last_file_id = last_file_id 
        self.id_block_map[self.last_file_id] = id_block_map
        for block in block_server_map:
            self.block_server_map[block] = block_server_map[block]
        self.file_info.append((self.last_file_id,name,size))
        self.store_info()
    
        globalvar.name2main_event.set()

    def quit_clear(self):
        # with open(infofile,"wb") as f:
        #     pickle.dump(self.file_info,f) 
        self.store_info()
        globalvar.name2main_event.set()
               

    def run(self):
        while True:
            globalvar.main2name_event.wait()
            globalvar.main2name_event.clear()
            if globalvar.OPERATION == globalvar.OPERATIONS["ls"]:
                self.assign_ls()
            if globalvar.OPERATION == globalvar.OPERATIONS["fetch"]:
                self.assign_fetch()
            if globalvar.OPERATION == globalvar.OPERATIONS["upload"]:
                self.assign_upload()
            if globalvar.OPERATION == globalvar.OPERATIONS["read"]:
                self.assign_read()
            if globalvar.OPERATION == globalvar.OPERATIONS["quit"]:
                self.quit_clear()
            