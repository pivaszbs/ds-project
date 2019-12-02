import rpyc
import os
import sys
import logging
import shutil
import http.client
from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"
PORT = 8888
logging.basicConfig(level=logging.DEBUG)

MASTER_IP = '18.216.31.245'
MASTER_PORT = 2131


class Minion(rpyc.Service):

    def exposed_check(self, file):
        file_path = os.path.join(DATA_DIR, file)
        if os.path.exists(file_path):
            return True
        else:
            return False

    def exposed_heart(self):
        return True

    def exposed_init(self):
        # arr = DATA_DIR.splt('\\')
        # dir = arr[-1] if arr[-1]!= '' else arr[-2]
        shutil.rmtree(DATA_DIR)
        os.mkdir(DATA_DIR)
        total, used, free = shutil.disk_usage("/")
        logging.debug("Free: %d GiB" % (free // (2**30)))


    def exposed_put(self, file, minions, source, flag):
        logging.debug("putting file: " + source)
        out_path = DATA_DIR
        source_path_arr = source.split('\\')
        for i in range(0,len(source_path_arr)-1):
            next_folder = source_path_arr[i]
            out_path = os.path.join(out_path,next_folder)
            if not os.path.exists(out_path):
                os.mkdir(out_path)

        out_path = os.path.join(out_path,source_path_arr[-1])

        if flag=='write':
            with open(out_path, 'wb') as f:
                f.write(file)
                logging.debug("file is stored successfully")
        elif flag=='make_dir':
            os.mkdir(out_path)
        elif flag=='create':
            open(out_path, 'a').close()

        if len(minions) > 0:
            self.forward(file, minions, source, flag)

    def forward(self, file, minions, source, flag):
        if flag=='write':
            text = 'file: '
        elif flag=='make_dir':
            text = 'directory: '
        elif flag=='create':
            text = 'empty file: '
        logging.debug("forwarding " + text + source + " to " + str(minions))
        next_minion = minions[0]
        minions = minions[1:]
        host, port = next_minion

        rpyc.connect(host, port=port).root.put(file, minions, source, flag)

    def exposed_get(self, file):
        logging.debug("get file: " + file)
        file_path = os.path.join(DATA_DIR, file)
        if not os.path.isfile(file_path):
            logging.debug("file not found")
            return None
        
        result = open(file_path, 'rb').read()
        return result

    def exposed_info(self, file):
        logging.debug("getting info about file " + os.path.basename(file))
        file_path = os.path.join(DATA_DIR, file)
        if not os.path.isfile(file_path):
            logging.debug("file not found")
            return None
        logging.critical('File is found, transfering info')
        result = os.stat(file_path)
        return result


    def exposed_delete(self, file_path, minions, flag):
        path = os.path.join(DATA_DIR, file_path)
        filename = os.path.basename(file_path)
        # shutil.rmtree(path)
        if flag == 'dir_delete_approved' or flag == 'delete_dir':
            shutil.rmtree(path) #for deleting directories
        else:
            # logging.critical(path)
            os.remove(path) #for deleting files
        logging.debug("file " + filename + " successfully deleted")
        if len(minions) > 0:
            self.forward_to_delete(file_path, minions, flag)

    def forward_to_delete(self, file_path, minions, flag):
        logging.debug("forwarding call to delete " + os.path.basename(file_path) + " to " + str(minions))
        next_minion = minions[0]
        minions = minions[1:]
        host, port = next_minion

        rpyc.connect(host, port=port).root.delete(file_path, minions, flag)

    def exposed_move(self, move_from, move_to, minions, flag):
        from_path = os.path.join(DATA_DIR, move_from)
        to_path = os.path.join(DATA_DIR, move_to)
        if not os.path.isfile(from_path):
            logging.debug("file not found")
            return None
        file = open(from_path, 'rb').read()
        with open(to_path, 'wb') as f:
            f.write(file)
        if flag == 'move':
            os.remove(from_path)
            logging.debug("file is moved successfully")
        elif flag == 'copy':
            logging.debug("file is copied successfully")
        if len(minions) > 0:
            self.forward_to_move(move_from, move_to, minions, flag)

    def forward_to_move(self, move_from, move_to, minions, flag):
        if flag == 'copy':
            text = 'copy '
        elif flag == 'move':
            text = 'move '
        logging.debug("forwarding call to "+ text + os.path.basename(move_from) + " to " + str(minions))
        next_minion = minions[0]
        minions = minions[1:]
        host, port = next_minion

        rpyc.connect(host, port=port).root.move(move_from, move_to, minions, flag)

if __name__ == "__main__":
    PORT = int(sys.argv[1])
    DATA_DIR = sys.argv[2]

    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)


    logging.debug("starting minion")
    rpyc_logger = logging.getLogger('rpyc')
    rpyc_logger.setLevel(logging.WARN)

    getIp = http.client.HTTPConnection("ifconfig.me")
    getIp.request("GET", "/ip")
    ip = getIp.getresponse().read()

    con = rpyc.connect(MASTER_IP, port=MASTER_PORT)
    result = con.root.get_updates(ip,PORT)
    if result:
        logging.debug('Getting updates...')
        for file, minions_with_file in result:
            file_path = os.path.join(DATA_DIR, file)
            dir_arr = file.split('/')[:-1]
            create_dir = DATA_DIR
            for dir in dir_arr:
                create_dir = os.path.join(create_dir, dir)
                if not os.path.isdir(create_dir):
                    os.mkdir(create_dir)

            if not os.path.exists(file_path):
                logging.debug('Getting file ' + file)
                for minion in minions_with_file:
                    host, port = minion
                    new_file = rpyc.connect(host, port=port).root.get(file)
                    if new_file is not None:#Will not work if file is empty
                        with open(file_path, 'wb') as f:
                            f.write(new_file)
                        break
                    else:
                        logging.critical('Unable to get file ' + str(file) + ' from any of the minions')

    t = ThreadedServer(Minion(), port=PORT,  logger=rpyc_logger, protocol_config={
    'allow_public_attrs': True,
})
    t.start()
