import rpyc
import sys
import os
import logging


logging.basicConfig(level=logging.DEBUG)


def read(master, file):
    minion_addr, file = master.read(file)
    if not minion_addr:
        logging.info("file not found")
        return
    filename = os.path.basename(file)
    for addr in minion_addr:
        logging.error("connecting to the minion:" + str(addr))
        host, port = addr
        try:
            con = rpyc.connect(host, port=port).root
            result = con.get(file)
            if result is not None:#Will not work if file is empty
                with open(filename, 'wb') as f:
                    f.write(result)
                break
        except:
            logging.error("unable to connect to the minion with host:" + host + " and port:" + port)
            continue
        else:
            logging.error("File does not exist")


def put(master, source, destination, flag):
    # size = os.path.getsize(source)#size in bytes
    """
    method master.write() == exposed_write() on master.py
    """
    minions, final_path = master.write(destination)
    if minions:
        for minion in minions:
            if flag=='write':
                file = open(source, 'rb').read()
            elif flag=='make_dir' or flag=='create':
                file = source
            minion = minions[0]
            minions = minions[1:]
            host, port = minion
            try:
                con = rpyc.connect(host, port=port)
                con.root.put(file, minions, final_path, flag)#put == exposed_put on minion
                break
            except ConnectionRefusedError:
                continue

def delete(master, source, flag):#for deleting files and directories
    responce = master.delete(source, flag)
    if flag == 'delete_dir':
        if responce:
            confirmation = input('Directory contain files. type Y to continue\n')
            if confirmation == 'Y':
                master.delete(source, 'dir_delete_approved')


def info(master, source):
    responce = master.info(source)
    print(responce)

def copy(master, source):
    responce = master.copy(source)
    print(responce)

def move(master, source, destination):
    responce = master.move(source, destination)
    print(responce)

def cd (master, dir):
    responce = master.cd(dir)
    print(responce)

def ls (master, dir):
    responce = master.ls(dir)
    print(responce)

def init (master):
    master.init()

def main(args):
    con = rpyc.connect("18.216.31.245", port=2131)
    master = con.root

    if args[0] == "init":
        init(master)
    elif args[0] == "create":
        put(master, args[1], args[1], 'create')
    elif args[0] == "write":
        put(master, args[1], args[2], 'write')
    elif args[0] == "read":
        read(master, args[1])
    elif args[0] == "delete_file":
        delete(master, args[1], 'delete_file')
    elif args[0] == "info":
        info(master, args[1])
    elif args[0] == "copy":
        copy(master, args[1])
    elif args[0] == "make_dir":
        put(master, args[1], args[1], 'make_dir')
    elif args[0] == "move":
        move(master, args[1], args[2])
    elif args[0] == "cd":
        cd(master, args[1])
    elif args[0] == "ls":
        try:
            path = args[1]
        except IndexError:
            path = ''
        ls(master, path)
    elif args[0] == "delete_dir":
        delete(master, args[1], 'delete_dir')
    else:
        logging.error("invalid command")


if __name__ == "__main__":
    main(sys.argv[1:])
