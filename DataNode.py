import globalvar
from globalconsts import *
from threading import Thread


class DataNode(Thread):
    def __init__(self, server_id):
        super(DataNode, self).__init__(name='DataServer%s' % (server_id,))
        self._server_id = server_id
    
    def run(self):
        while(True):
            globalvar.name2data_events[self._server_id].wait()
            if globalvar.OPERATION == OPERATIONS["fetch"]:
                self.fetch()
            elif globalvar.OPERATION == OPERATIONS["upload"]:
                self.upload()
            elif globalvar.OPERATION == OPERATIONS["read"]:
                self.read()
            else:
                pass
            #globalvar.data2name_events[self._server_id].set()


    def fetch(self):
        if self._server_id in globalvar.fetch_server_block_map:

            blocks_to_be_fetch = globalvar.fetch_server_block_map[self._server_id]
        
            data = None
            for block in blocks_to_be_fetch:
                with open(DataNode_path + str(self._server_id)+"/" + block,"rb") as f_in:
                    #f_in.seek(start,0)
                    data = f_in.read()
                globalvar.fetch_results[block] = data
        else:
            pass
        globalvar.name2data_events[self._server_id].clear()
        globalvar.data2name_events[self._server_id].set()

    def read(self):
        if self._server_id not in globalvar.read_server_block_map:
            pass
        else:

            blocks = globalvar.read_server_block_map[self._server_id]
            #print("read_block_config:",globalvar.read_block_config[block[0]])
            globalvar.read_results.clear()
            for block in blocks:
                offset = globalvar.read_block_config[block][0]
                count = globalvar.read_block_config[block][1]
                data = None
            #print(offset,count)
                with open(file=  DataNode_path + str(self._server_id) +"/"+ str(block),mode = "rb") as f:
                    f.seek(offset,0)
                    data = f.read(count)
                globalvar.read_results[block] = data
        globalvar.name2data_events[self._server_id].clear()
        globalvar.data2name_events[self._server_id].set()

    def upload(self):
        filepath = globalvar.upload_file
        if self._server_id not in globalvar.upload_server_block_map:
            pass
        else:
            block_to_be_save = globalvar.upload_server_block_map[self._server_id]
            #print(block_to_be_save)
            with open(filepath,"rb") as f_in:
                for block_entry in block_to_be_save:
                    file_id = block_entry[0]
                    offset = block_entry[1]
                    file_size = block_entry[2]
                    block_id = block_entry[3]
                    f_in.seek(offset,0)
                    data = f_in.read(file_size)
                    with open("./dfs/DataNode%d/"%(self._server_id,) + "%d-part-%d"%(file_id,block_id),"wb") as f_out:
                        f_out.write(data)
                        f_out.flush()
        globalvar.name2data_events[self._server_id].clear()
        globalvar.data2name_events[self._server_id].set()