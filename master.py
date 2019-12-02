import rpyc
import uuid
import math
import random
import configparser
import signal
import pickle
import sys
import os
import logging
from datetime import datetime
from threading import Thread
import time

from rpyc.utils.server import ThreadedServer

BLOCK_SIZE = 1000
REPLICATION_FACTOR = 0
id = 0

MINIONS = {"1": ("18.219.69.40", 8000),
           "2": ("3.14.10.243", 9000),
           "3": ("3.14.82.18", 10000)}
MINIONS = {}

class MasterService(rpyc.Service):
    """
    file_block = {'file.txt': ["block1", "block2"]}
    block_minion = {"block1": [1,3]}
    minions = {"1": (127.0.0.1,8000), "3": (127.0.0.1,9000)}
    """

    # file_block = {}
    # block_minion = {}
    # file = {}
    file_minions = {} # {file_name: [1,3]}
    # lost_file_minions = {}
    minions = MINIONS
    dead_minions = {}
    block_size = BLOCK_SIZE
    replication_factor = REPLICATION_FACTOR
    id = id
    current_dir = ''

    def heartbeat(self):
        while True:
            time.sleep(5)
            interval = Thread(target=self.heartbeat_python_govno)
            interval.start()
            interval.join()
                
    def heartbeat_python_govno(self):
        del_id = []
        for id, minion in self.minions.items():
            host, port = minion
            try:
                con = rpyc.connect(host, port=port)
                minion_connection = con.root
                result = minion_connection.heart()
            except Exception as e:
                logging.critical(str(host) + ':' + str(port) + ' is dead')
                self.dead_minions[id] = minion
                del_id.append(id)
        for delete in del_id:
            del self.minions[delete]
            self.replication_factor -= 1
            

    def __init__(self, *args): 
        rpyc.Service.__init__(self, *args)
        thread1 = Thread(target=self.heartbeat)
        thread1.start()


    def exposed_get_updates(self, minion_ip, minion_port):
        minion_ip = minion_ip.decode("utf-8")
        # new_minion = (minion_ip, minion_port)
        new_minion = (minion_ip, minion_port)
        for id, minion in self.dead_minions.items():
            if minion == new_minion:
                self.minions[id] = new_minion
                del self.dead_minions[id]
                break
        else:
            self.minions[self.id] = new_minion
            self.id += 1
            self.replication_factor += 1
                
        for id, minion in self.minions.items():
            # logging.critical(minion)
            if minion == new_minion:
                files = []
                for file, id_arr in self.file_minions.items():
                    if id in id_arr:
                        other_minions = [self.minions[other_id] for other_id in id_arr if id != other_id]
                        files.append((file, other_minions))
                if files:
                    return files
                else:
                    logging.critical('Minion ' + str(new_minion) + ' is up to date')
                    return None



    def path_parser(self, path):
        dir_arr = path.split('/')
        temporary_current_dir = self.current_dir
        for dir in dir_arr:
            if dir == '.':
                continue
            elif dir == '..':
                if temporary_current_dir:
                    bound = temporary_current_dir.rfind('/')
                    if bound != -1:
                        temporary_current_dir = temporary_current_dir[:bound]#go to the father dir
                    else:
                        temporary_current_dir = ''
                else:
                    logging.critical('Directory does not exist')
                    return None
            else:
                temporary_current_dir = os.path.join(temporary_current_dir, dir)#dir addition
                path_exist = self.file_minions.get(temporary_current_dir)
                if not path_exist:
                    logging.critical('Directory does not exist')
                    return None
        return temporary_current_dir

    def exposed_init(self):
        minion_addr = list(self.minions.values())
        for minion in minion_addr:
            logging.critical(minion)
            host, port = minion
            con = rpyc.connect(host, port=port)
            con.root.init()

    def exposed_ls(self, path):
        dir_to_ls = os.path.join(self.current_dir, path)
        if dir_to_ls.endswith('/'):
            dir_to_ls = dir_to_ls[:-1]
        logging.critical(dir_to_ls)
        dir_content = []
        for key in self.file_minions.keys():
            if key.startswith(dir_to_ls) or dir_to_ls == '':
                dir_content.append(key)
        if not dir_content:
            logging.critical('Chosen directory does not exist')
            return 'Chosen directory does not exist'
        else:
            logging.critical('Content of the chosen directory is:')
            log = 'Content of the chosen directory is: \n'
            for content in dir_content:
                new_cont = content.replace(dir_to_ls,'',1)
                if new_cont.find('/') == 0:
                    new_cont = new_cont[1:]
                if new_cont.find('/') == -1:
                    logging.critical(new_cont)
                    log += new_cont + '\n'
            return log

    def exposed_cd(self, path):

        new_dir = self.path_parser(path)
        if new_dir or new_dir=='':
            self.current_dir = new_dir
            logging.critical('You successfully changed directory to \\' + self.current_dir)
            return 'You successfully changed directory to \\' + self.current_dir
        else:
            return

    def exposed_read(self, path):
        path = os.path.join(self.current_dir, path)
        minion_addr = [self.minions[id] for id in self.file_minions.get(path)]

        return minion_addr, path

    def exposed_write(self, path):
        path = os.path.join(self.current_dir, path)
        bound = path.rfind('/')
        logging.critical(path)
        if bound==-1 or self.file_minions.get(path[:bound]):
            minion_ids = random.sample(     # allocate REPLICATION_FACTOR number of minions
                    list(self.minions.keys()), self.replication_factor)
            minion_addr = [self.minions[m] for m in minion_ids] #[('127.0.0.1', 9000), ('127.0.0.1', 8000)]
            self.file_minions[path] = minion_ids
            logging.critical('current file table:' + str(self.file_minions))
            return minion_addr, path
        else:
            logging.critical('Directory where you want to place file does not exist')
            return False

    def exposed_delete(self, path, flag):
        path = os.path.join(self.current_dir, path)
        minion_ids = self.file_minions.get(path)
        if not minion_ids:
            logging.critical("file not found")
            return
        if flag == 'delete_dir':
            for key in self.file_minions.keys():
                key_contains_path = len(key.split(path)) >=2
                dir_is_empty = key.split(path)[-1] == ''
                if key_contains_path and not dir_is_empty:
                    return 'ALO DIR NE PYSTAYA YSPOKOISYA IDI SPAT'

        del self.file_minions[path]

        if flag == 'dir_delete_approved':
            keys_to_delete = []
            for key in self.file_minions.keys():
                key_contains_path = len(key.split(path)) >=2
                if key_contains_path:
                    keys_to_delete.append(key)
            for key in keys_to_delete:
                del self.file_minions[key]

        minion_addr = [self.minions[m] for m in minion_ids]
        logging.critical('current file table:' + str(self.file_minions))
        minion = minion_addr[0]
        minions = minion_addr[1:]
        host, port = minion
        con = rpyc.connect(host, port=port)
        con.root.delete(path, minions, flag)

    def exposed_copy(self, path):
        path = os.path.join(self.current_dir, path)
        minion_ids = self.file_minions.get(path)
        if not minion_ids:
            logging.critical("file not found")
            return

        dot = path.rfind('.')
        copy_iterator = 1
        check_name = path[:dot] + '_' + str(copy_iterator)
        while self.file_minions.get(check_name + path[dot:]):
            copy_iterator+=1
            check_name = path[:dot] + '_' + str(copy_iterator)
        new_path = check_name + path[dot:]
        self.file_minions[new_path] = minion_ids
        logging.critical('current file table:' + str(self.file_minions))
        minion_addr = [self.minions[m] for m in minion_ids]
        minion = minion_addr[0]
        minions = minion_addr[1:]
        host, port = minion
        con = rpyc.connect(host, port=port)
        con.root.move(path, new_path, minions, 'copy')

    def exposed_move(self, move_from, move_to):
        move_from = os.path.join(self.current_dir, move_from)
        minion_ids = self.file_minions.get(move_from)
        if not minion_ids:
            logging.critical("file not found")
            return

        move_to_name = os.path.basename(move_to)
        path_dir = os.path.dirname(move_to)
        move_to = self.path_parser(path_dir)
        if move_to == '' or self.file_minions.get(move_to):
            move_to = os.path.join(move_to,move_to_name)

            self.file_minions[move_to] = minion_ids
            del self.file_minions[move_from]
            logging.critical('current file table:' + str(self.file_minions))
            minion_addr = [self.minions[m] for m in minion_ids]
            minion = minion_addr[0]
            minions = minion_addr[1:]
            host, port = minion
            con = rpyc.connect(host, port=port)
            con.root.move(move_from, move_to, minions, 'move')
        else:
            logging.critical('Path where you want to place file does not exist')

    def exposed_info(self, path):
        path = os.path.join(self.current_dir, path)
        try:
            minion_ids = self.file_minions[path]
            minion_addr = [self.minions[m] for m in minion_ids]

            for addr in minion_addr:
                logging.error("connecting to the minion:" + str(addr))
                host, port = addr
                try:
                    con = rpyc.connect(host, port=port).root
                    file = con.info(path)
                    if file:
                        log = "file size:" + str(math.ceil(file.st_size/1024)) + "KB\n"
                        logging.critical("file size:" + str(math.ceil(file.st_size/1024)) + "KB")
                        modification_time = datetime.utcfromtimestamp(file.st_mtime + 3*60*60).strftime('%Y-%m-%d %H:%M:%S')
                        logging.critical("Most recent content modification:" + modification_time)
                        log += "Most recent content modification:" + modification_time
                        return log
                except Exception as e:
                    # logging.error(e)
                    logging.error("unable to connect to the chosen minion.")
                    continue
                else:
                    logging.error("File does not exist at storage servers")
        except:
            logging.error("There is no such file")
            return "There is no such file"


if __name__ == "__main__":
    t = ThreadedServer(MasterService(), port=2131, protocol_config={
    'allow_public_attrs': True,
})
    t.start()
