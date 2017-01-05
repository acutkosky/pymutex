'''implements locking for sharing resources
between pythong processes'''

import os
import pickle
import time
import random

LOCK_SUFFIX = '.lock'

DEFAULT_EXPIRY = 10 #locks expire after 10 secs by default

ID = os.getpid() * 1000 + int(random.random()*1000)


def touch_path(path):
    '''creates directories for given scope'''
    try:
        os.makedirs(path)
    except OSError:
        pass

def get_timestamp():
    '''returns a timestamp'''
    return time.time()

def get_lock_dir_prefix(lockstring):
    '''gets directory and filename prefix for lockfile'''
    lock_dir = os.path.dirname(lockstring)
    basename = os.path.basename(lockstring)
    lockfile_prefix = basename + str(hash(basename))
    return lock_dir, lockfile_prefix

def get_lockfile_name(lockstring):
    '''gets the name of file used to claim the lock'''
    lock_dir, lockfile_prefix = get_lock_dir_prefix(lockstring)
    return os.path.join(lock_dir, lockfile_prefix + ':' + str(ID) + LOCK_SUFFIX)

def get_lock_holder(lockstring, timeout):
    '''finds the ID of the process that currently holds the lock'''
    lock_dir, lockfile_prefix = get_lock_dir_prefix(lockstring)
    touch_path(lock_dir)
    file_names = [f for f in os.listdir(lock_dir) if \
        os.path.isfile(os.path.join(lock_dir, f)) and f.find(lockfile_prefix) == 0]

    current_time = get_timestamp()

    expired_locks = []

    def get_time_id_data(file_names):
        '''helper function for extracting the list of processes (by ID) waiting on the lock
        and when each ID asked for the lock.'''
        for file_name in file_names:
            file_pointer = open(os.path.join(lock_dir, file_name))
            try:
                time_id = pickle.load(file_pointer)
                if time_id['timestamp'] > current_time + timeout:
                    expired_locks.append(file_name)
            except (EOFError, ValueError):
                time_id = None
            file_pointer.close()
            if time_id != None:
                yield time_id

    time_id_data = [data for data in get_time_id_data(file_names)]

    if len(time_id_data) == 0:
        return -1

    min_time_stamp = min([x['timestamp'] for x in time_id_data if \
        x['timestamp'] < current_time + timeout])
    lock_holder = min([x['ID'] for x in time_id_data if x['timestamp'] == min_time_stamp])

    for file_name in expired_locks:
        os.remove(os.path.join(lock_dir, file_name))

    return lock_holder

def locked_by_us(lockstring, timeout=DEFAULT_EXPIRY):
    '''returns true if this process holds the lock.'''
    return get_lock_holder(lockstring, timeout) == ID

def unlock(lockstring):
    '''release the lock.'''
    try:
        lockfile_name = get_lockfile_name(lockstring)
        os.remove(lockfile_name)
    except OSError:
        pass

def lock(lockstring, timeout=DEFAULT_EXPIRY):
    '''acquire the lock.
    Returns when the lock has been acquired.

    BUSY WAITS FOR NOW - IN FUTURE THIS SHOULD PROBABLY BECOME ASYNC
    '''

    #Prevent double-locking
    print "lock"
    assert not locked_by_us(lockstring, timeout)

    lockfile_name = get_lockfile_name(lockstring)
    lockfile = open(lockfile_name, 'w')
    timestamp = get_timestamp()
    pickle.dump({'timestamp': timestamp, 'ID': ID}, lockfile)
    lockfile.close()

    while not locked_by_us(lockstring, timeout):
        pass
