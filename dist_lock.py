#encoding=utf-8
import pydoop.hdfs as hdfs
import os

class DistLock(object):
    """
    DistLock是基于HDFS实现的分布式锁机制。
    该锁的目的是方便HDFS文件锁操作，但是适用于对锁效率要求不高的场景。
    主要优势是：分布式锁创建机制，配置简单
    """
  
    def __init__(self, name = "lock",
                       host = "default",
                       port = 0,
                       user = None):
       self.name = name
       self.hdfs = hdfs.hdfs(host, port, user)
       self.hdfs.create_directory(self.name)
       self.lock_path = None

    def __del__(self):
        self.hdfs.delete(self.name, True)
        self.hdfs.close()

    def fetch_lock(self, id = None):
        try:
            if id == None: return False
            path = "%s/%s" % (self.name, id)
            if not self.hdfs.exists(path):
                nfile = self.hdfs.open_file(path, "w")
                nfile.close()

            time.sleep(2) # hdfs last_mod的时间单位为秒，保证一秒中同时创建两个文件，都能被看到
            file_info_list = self.hdfs.list_directory(self.name)

            #print file_info_list
            lock_path = None
            lock_time = None

            for file_info in file_info_list:
                if (lock_path == None) or \
                (lock_time > file_info['last_mod']):
                    lock_path = file_info['path']
                    lock_time = file_info['last_mod']

            last_slash_index = lock_path.rindex('/')
            lock_id = lock_path[last_slash_index + 1:]

#            print "%s %s" % (lock_path, lock_id)
            if lock_id == id:
                self.lock_path = lock_path 
                return True
            else:
                return False
        except Exception:
            return False

    def release_lock(self):
        if self.lock_path == None:
            return
        else:
            self.hdfs.delete(self.lock_path)
        

if __name__ == "__main__":
    dist_lock = DistLock()

    import threading
    import random
    import time
    def func(id):
        time.sleep(random.randint(0,10))
        
        print "%s try to fetch lock" % id
        while not dist_lock.fetch_lock(id):
            time.sleep(2)
    
        print "%s fetch lock, at time %s" % (id, time.localtime())
        print "%s do something..." % id
        time.sleep(random.randint(0,2))
        dist_lock.release_lock()
        print "%s release lock, at time %s" % (id, time.localtime())
    
    
    for i in range(0, 10):
        th = threading.Thread(target=func, args=("worker_%s" % i,))
        th.start()