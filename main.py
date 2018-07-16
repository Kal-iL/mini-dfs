import os
from globalconsts import *
import globalvar
import NameNode
import DataNode

cmd_prompt = 'Mini-DFS >> '

usages = "Usages:\n\tls:\n\t\tlist all files on dfs\n\tupload file_name:\n\t\tupload a file to dfs\n\tread file_id offset count:\n\t\tread from a file from dfs\n\tfetch file_id save_path:\n\t\tfetch a file from dfs to local\n\tquit:\n\t\tquit from dfs"

PRINT_PATTERN = "%10s\t%20s\t%20s\t"

def parse_cmds(cmds):
    Flag = False
    cmd = cmds.split()
    if cmd[0] not in operation_names:
        print(usages)

    elif cmd[0] == "ls":
        if len(cmd) != 1:
            print("Usage: ls")
        else:
            globalvar.OPERATION = OPERATIONS["ls"]
            Flag = True
    elif cmd[0] == "fetch":
        if len(cmd) != 3:
            print("Usage: fetch file_id save_path")
        else:
            try:
                file_id = int(cmd[1])
            except ValueError:
                print("file_id must be integer")
            else:
                save_file = cmd[2]
                save_path = os.path.split(save_file)[0]
                if not os.path.exists(save_path):
                    print("Save Path does not exist")
                else:
                    globalvar.OPERATION = OPERATIONS["fetch"]
                    globalvar.fetch_save_file = save_file
                    globalvar.fetch_file_id = file_id
                    Flag = True

    elif cmd[0] == "upload":
        if len(cmd) != 2:
            print("Usage: upload file")
        else:
            if os.path.isfile(cmd[1]):
                globalvar.OPERATION = OPERATIONS["upload"]
                globalvar.upload_file = cmd[1]
                Flag = True
            else: 
                print("file does not exist")
    elif cmd[0] == "read":
        if len(cmd) != 4:
            print("Usage: read file_id offset count")
        else:
            try:
                file_id = int(cmd[1])
                offset = int(cmd[2])
                count = int(cmd[3])
            except ValueError:
                print("file_id,offset,count must be integer")
            else:
                globalvar.OPERATION = OPERATIONS["read"]
                globalvar.read_file_id = file_id
                globalvar.read_offset = offset
                globalvar.read_count = count
                Flag = True
    elif cmd[0] == "quit":
        if len(cmd) != 1:
            print("Usage: quit")
        else:
            os._exit(0)
            Flag = True
    return Flag

def get_results():
    if globalvar.OPERATION == OPERATIONS["read"]:
        #num_of_blocks = len(globalvar.read_results)
        #print(num_of_blocks)
        #file_id = globalvar.read_file_id
        data = bytes()
        #print(globalvar.read_results)
        results = globalvar.read_results
        #print(results)
        #data = bytearray()
        # globalvar.read_results.clear()
        blocks = list(results.keys())
        blocks.sort()
        for block in blocks:
            data_i = results[block]
            # try:
            #     data = data.decode(encoding = "utf-8")
            # except UnicodeDecodeError :
            #     #print(len(data))
            #     pass
            data = data+data_i
        #print(data,end = "")
        #print('\n')
        print(data)
            
        #print(globalvar.fetch_results)

    elif globalvar.OPERATION == OPERATIONS["fetch"]:
        #
        # num_of_blocks = len(globalvar.fetch_results)
        # file_id = globalvar.fetch_file_id
        results = globalvar.fetch_results
        with open(globalvar.fetch_save_file,"wb") as f_out:
            for block in results:
                f_out.write(results[block])

    elif globalvar.OPERATION == OPERATIONS["ls"]:
        total = len(globalvar.ls_results)
        print("total: ",total)
        if total > 0:
            print(PRINT_PATTERN % ("ID","File Name","File Size"))
            for entry in globalvar.ls_results:
                print(PRINT_PATTERN % (entry[0],entry[1],str(entry[2])))
        else:
            pass
    
def run():
    # if not os.path.exists("./dfs"):
    #     os.makedirs("./dfs")
    name_server = NameNode.NameNode('NameServer')
    name_server.start()

    data_servers = [DataNode.DataNode(i) for i in range(NUM_OF_DATASERVERS)]
    for server in data_servers:
        server.start()
    
    for i in range(NUM_OF_DATASERVERS):
        if not os.path.exists(DataNode_path + str(i)):
            os.makedirs(DataNode_path + str(i))
    
    if not os.path.exists(NameNode_path):
        os.makedirs(NameNode_path)
    
    while True:
        print(cmd_prompt,end = "")
        cmd = input()
        if not parse_cmds(cmd):
            #print(usages)
            continue
        globalvar.main2name_event.set()

        globalvar.name2main_event.wait()

        globalvar.name2main_event.clear()

        # if globalvar.NameNode_Flag:
        #     continue

        get_results()

        
        
        # if globalvar.OPERATION == OPERATIONS["quit"]:
        #     os._exit(0)

if __name__ == "__main__":
    run()