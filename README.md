# ds-project

# Components:
 1. **Master :** Contain metadata
 2. **Minion :** Contain files 
 3. **Client :** Interacts with Master and Minion
 
 ## Client interface:
 
**init** - clean everything

 **create file.name** -creates empty file
 
 **write path/from/user.space place/to/put.txt** - load file to DFS
 
 **read file.name** - download file to client
 
 **delete_file file.name** - delete file from DFS
 
 **info file.name** - get info of the file
 
 **copy  file.name** - creates copy of the file 
 
 **make_dir dir** -creates dir
 
 **move from/file.path to/file.path** - move file
 
 **cd** - cd
 
 **ls** - ls
 
 All necessary diagrams are presented in the presentation

 **мастер:**
 ```sudo docker pull pivaszbs/dfs-project-123 && sudo docker run -p 2131:2131 pivaszbs/dfs-project-123 python master.py```

 **миньон**
 ```sudo docker pull pivaszbs/dfs-project-123 && sudo docker run -p 9000:9000 pivaszbs/dfs-project-123 python minion.py 9000 /tmp/m1```