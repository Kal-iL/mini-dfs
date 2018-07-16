# mini-dfs
This is a python3 implemented mini distributed file system, we used threads to mimic NameServer and DataServers and Clients

## Baisc Functions
### upload: upload a file to dfs
### fetch: download a file from dfs to local
### read: read the content of a file in dfs
### ls: list all the files in dfs

## Main Feature:

### We use a globalvar module to share information between DataServers and NameServer
###

## Usages:
### To start:
	python3 main.py

### ls
	Mini-DFS >> ls
### upload 
	Mini-DFS >> upload file_path
### fetch 
	Mini-DFS >> fetch file_id save_path
### read 
	Mini-DFS >> read file_id offset cout
