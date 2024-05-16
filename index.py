import os
import uuid
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
next_bucket = 1

def save_last_key(key):
    with open("last_key.txt", "w") as f:
        f.write(str(key))
        f.close()

def get_last_key():
    try:
        with open("last_key.txt", "r") as f:
            return int(f.read())
    except FileNotFoundError:
        return None

def insert_data(value):
    global next_bucket
    key = get_last_key() + 1 if get_last_key() else 1
    comm.send({'action': 'insert', 'key': key, 'value': value}, dest=next_bucket)
    next_bucket = (next_bucket % (size - 1)) + 1
    save_last_key(key)

def r_insert_data(key, value):
    global next_bucket
    comm.send({'action': 'insert', 'key': key, 'value': value}, dest=next_bucket)
    next_bucket = (next_bucket % (size - 1)) + 1
    save_last_key(key)

def delete_data(key):
    for bucket in range(1, size):
        comm.send({'action': 'delete', 'key': key}, dest=bucket)

def get_data(key):
    for bucket in range(1, size):
        comm.send({'action': 'get', 'key': key}, dest=bucket)

def set_data(key, value):
    for bucket in range(1, size):
        comm.send({'action': 'update', 'key': key, 'value': value}, dest=bucket)

def find_data(key):
    for bucket in range(1, size):
        comm.send({'action': 'find', 'key': key}, dest=bucket)

def load_last_key():
    try:
        with open("last_key.txt", "r") as f:
            return f.read()
    except FileNotFoundError:
        return None

def find_in_bucket(key):
    found = False

    with open(f"bucket_{rank}.txt", "r") as f:
        for line in f:
            id, value = line.strip().split(':')
            if key == id:
                found = True
                break

    return found

def get_from_bucket(key):
    value = None

    with open(f"bucket_{rank}.txt", "r") as f:
        for line in f:
            id, val = line.strip().split(':')
            if key == id:
                value = val
                break
   
    return value

def delete_from_bucket(key):
    with open(f"bucket_{rank}.txt", "r+") as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            id, value = line.strip().split(':')
            if key != id:
                f.write(line)
        f.truncate()
    

def update_in_bucket(key, new_value):
    with open(f"bucket_{rank}.txt", "r+") as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            id, value = line.strip().split(':')
            if key == id:
                f.write(f"{id}:{new_value}\n")
            else:
                f.write(line)
        f.truncate()

def create_bucket():
    for i in range(1, size):
        if not os.path.exists(f"bucket_{i}.txt"):
            open(f"bucket_{i}.txt", "w").close()

def check_if_balance():
    list_of_buckets = [f"bucket_{i}.txt" for i in range(1, size)]
    map_lines = {}
    all_lines = 0

    for file in list_of_buckets:
        try:
            with open(file) as f:
                file_lines = len(f.readlines())
                f.close()
            map_lines[file] = file_lines
            all_lines += file_lines
        except FileNotFoundError:
            map_lines[file] = 0

    print(f"All lines: {all_lines}")

    if all_lines < size:
        return True, map_lines

    average_lines = all_lines / len(map_lines)

    for file, lines in map_lines.items():
        if lines > 1.3 * average_lines or lines < 0.7 * average_lines:
            return False, map_lines

    return True, map_lines

def rebalance_data(map_lines):
    all_data = []

    for file in map_lines.keys():
        try:
            with open(file, "r") as f:
                all_data.extend(f.readlines())
        except FileNotFoundError:
            pass

    if len(all_data) == 0:
        return

    # delete all files
    for file in map_lines.keys():
        open(file, "w").close()

    print(f"Data before rebalance: ")

    for key, value in map_lines.items():
        print(f"Bucket {key} has {value} lines.")

    # redistribute data
    for data in all_data:
        key, value = data.strip().split(':')
        r_insert_data(key, value)

def save_in_bucket(key, value):
    with open(f"bucket_{rank}.txt", "a") as f:
        f.write(f"{key}:{value}\n")
        f.close()

if rank == 0:
    create_bucket()

    for i in range(10000):
        insert_data(str(uuid.uuid4()))
    
    get_data("1")
    
    set_data("1", "new_value")

    get_data("1")

    find_data("2")

    delete_data("2")

    find_data("2")

    balance, map_lines = check_if_balance()
    
    print(f"Is the data balanced? {balance}")
    
    while not balance:
        rebalance_data(map_lines)
        balance, map_lines = check_if_balance()
        print(f"Is the data balanced? {balance}")
    
    for key, value in map_lines.items():
        print(f"Bucket {key} has {value} lines.")
    
    for i in range(1, size):
        comm.send({'action': 'stop'}, dest=i)
else:
    while True:
        data = comm.recv(source=0)
        
        if data['action'] == 'stop':
            break
        elif data['action'] == 'insert':
            save_in_bucket(data['key'], data['value'])
        elif data['action'] == 'delete':
            delete_from_bucket(data['key'])
        elif data['action'] == 'update':
            update_in_bucket(data['key'], data['value'])
        elif data['action'] == 'get':
            res = get_from_bucket(data['key'])

            if res:
                print(f"Data for key {data['key']} is: {res}")

        elif data['action'] == 'find':
            found = find_in_bucket(data['key'])

            if found:
                print(f"Key {data['key']} found in bucket {rank}: {found}")

comm.Barrier()