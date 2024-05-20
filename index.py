import os
import uuid
import sys
from mpi4py import MPI

# Initialize MPI communication
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def save_config(next_bucket, next_key):
    with open("config.txt", "w") as file:
        file.write(f"next_bucket={next_bucket}\n")
        file.write(f"next_key={next_key}\n")

def get_config():
    config = {}
    try:
        with open("config.txt", "r") as file:
            for line in file:
                key, value = line.strip().split('=')
                config[key] = int(value)
    except FileNotFoundError:
        config['next_bucket'] = 1
        config['next_key'] = 1
    return config

config = get_config()

def insert_data(value):
    global config
    key = config['next_key']
    next_bucket = config['next_bucket']
    comm.send({'action': 'insert', 'key': key, 'value': value}, dest=next_bucket)
    config['next_bucket'] = (next_bucket % (size - 1)) + 1
    config['next_key'] += 1

def delete_data(key):
    success = False
    
    for bucket in range(1, size):
        comm.send({'action': 'delete', 'key': key}, dest=bucket)
    
    for i in range(1, size):
        message = comm.recv(source=i)
        if message['action'] == 'delete_result':
            if message['success']:
                success = True
    
    if success:
        print(f"[DELETE] Data with key {key} has been deleted")
    else:
        print(f"[DELETE] Data with key {key} not found")

def get_data(key):
    data = None
    
    for i in range(1, size):
        comm.send({'action': 'get', 'key': key}, dest=i)
        
    for i in range(1, size):
        message = comm.recv(source=i)
        if message['action'] == 'get_result':
            if message['value']:
                data = message['value']
    if data:
        print(f"[GET] Data with key {key} is {data}")
    else:
        print(f"[GET] Data with key {key} not found")
            

def set_data(key, value):
    success = False
    
    for bucket in range(1, size):
        comm.send({'action': 'set', 'key': key, 'value': value}, dest=bucket)

    for i in range(1, size):
        message = comm.recv(source=i)
        if message['action'] == 'set_result':
            if message['success']:
                success = True
            
    if success:
        print(f"[SET] Data with key {key} has been updated")
    else:
        print(f"[SET] Data with key {key} not found")

def find_data(key):
    data = None

    for i in range(1, size):
        comm.send({'action': 'find', 'key': key}, dest=i)

    for i in range(1, size):
        message = comm.recv(source=i)
        if message['action'] == 'find_result':
            if message['found']:
                data = True
        
    if data:
        print(f"[FIND] The key {key} exists")
    else:
        print(f"[FIND] The key {key} does not exist")
    
def delete_from_bucket(key):
    found_and_deleted = False
    
    try:
        with open(f"bucket_{rank}.txt", "r+") as file:
            lines = file.readlines()
            file.seek(0)
            for line in lines:
                id, value = line.strip().split(':')
                if int(key) == int(id):
                    found_and_deleted = True
                else:
                    file.write(line)
            file.truncate()
    except FileNotFoundError:
        pass

    return found_and_deleted

def get_from_bucket(key):
    try:
        with open(f"bucket_{rank}.txt", "r") as file:
            for line in file:
                id, val = line.strip().split(':')
                if str(key) == str(id):
                    return val
    except FileNotFoundError:
        pass
    return None

def set_in_bucket(key, new_value):
    found_and_updated = False
    
    try:
        with open(f"bucket_{rank}.txt", "r+") as file:
            lines = file.readlines()
            file.seek(0)
            for line in lines:
                id, value = line.strip().split(':')
                if int(key) == int(id):
                    found_and_updated = True
                    file.write(f"{id}:{new_value}\n")
                else:
                    file.write(line)
            file.truncate()
    except FileNotFoundError:
        pass

    return found_and_updated

def find_in_bucket(key):
    try:
        with open(f"bucket_{rank}.txt", "r") as file:
            for line in file:
                id, value = line.strip().split(':')
                if str(key) == str(id):
                    return True
    except FileNotFoundError:
        pass
    return False

def create_buckets():
    for i in range(1, size):
        if not os.path.exists(f"bucket_{i}.txt"):
            open(f"bucket_{i}.txt", "w").close()

def save_to_bucket(key, value):
    with open(f"bucket_{rank}.txt", "a") as file:
        file.write(f"{key}:{value}\n")

def rebalance_buckets():
    total_lines = 0
    lines_map = {}

    for i in range(1, size):
        comm.send({'action': 'check_balance'}, dest=i)

    for i in range(1, size):
        message = comm.recv(source=i)
        if message['action'] == 'balance':
            total_lines += message['total_lines']
            lines_map[message['rank']] = message['total_lines']
        
    avg_lines = total_lines // (size - 1)
    avg_70 = round(avg_lines * 0.7)
    avg_130 = round(avg_lines * 1.3)

    need_rebalance = any(lines < avg_70 or lines > avg_130 for lines in lines_map.values())

    if not need_rebalance:
        print("[REBALANCE] No need to rebalance")
        return

    print(f"[REBALANCE] Total lines: {total_lines}")
    print(f"[REBALANCE] Avg lines: {avg_lines}")
    print(f"[REBALANCE] Avg 70%: {avg_70}")
    print(f"[REBALANCE] Avg 130%: {avg_130}")

    for rank, lines in lines_map.items():
        print(f"[REBALANCE] Bucket {rank} has {lines} lines")

    new_lines_map = {rank: avg_lines - lines for rank, lines in lines_map.items()}
    donors = {rank: -value for rank, value in new_lines_map.items() if value < 0}
    receivers = {rank: value for rank, value in new_lines_map.items() if value > 0}

    print(f"[REBALANCE] Donors: {donors}")
    print(f"[REBALANCE] Receivers: {receivers}")

    total_lines_receives = sum(receivers.values())
    total_lines_donors = sum(donors.values())
    operations = []

    valid_operations = min(total_lines_receives, total_lines_donors)

    while valid_operations > 0:
        for donor, value in donors.items():
            for receiver, needed in receivers.items():
                if value > 0 and needed > 0:
                    operations.append({'donor': donor, 'receiver': receiver})
                    donors[donor] -= 1
                    receivers[receiver] -= 1
                    valid_operations -= 1

    for operation in operations:
        print(f'[REBALANCE] Donor {operation["donor"]} will donate to {operation["receiver"]}')
        comm.send({'action': 'donate', 'receiver': operation['receiver']}, dest=operation['donor'])

argv = sys.argv

if rank == 0:
    commands = ['INSERT_RND', 'GET', 'FIND', 'SET', 'DELETE', 'CLEAR']
    
    if len(argv) > 1 and argv[1] == 'CLEAR':
        for i in range(1, size):
            if os.path.exists(f"bucket_{i}.txt"):
                os.remove(f"bucket_{i}.txt")
        if os.path.exists("config.txt"):
            os.remove("config.txt")
        
    
    create_buckets()

    if len(argv) > 1:
        operation = argv[1]

        if operation not in commands:
            print(f"Invalid command. Valid commands are {commands}")
            sys.exit(1)
        
        if operation == 'INSERT_RND':
            amount = int(argv[2])
            for i in range(amount):
                insert_data(str(uuid.uuid4()))
        elif operation == 'GET':
            key = int(argv[2])
            get_data(key)
        elif operation == 'FIND':
            key = int(argv[2])
            find_data(key)
        elif operation == 'SET':
            key = int(argv[2])
            value = argv[3]
            set_data(key, value)
        elif operation == 'DELETE':
            key = int(argv[2])
            delete_data(key)

    # Check and rebalance buckets
    rebalance_buckets()
    save_config(config['next_bucket'], config['next_key'])
else:
    while True:
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, status=status)

        if message['action'] == 'stop':
            break
        elif message['action'] == 'insert':
            print(f"Inserting data {message['value']} in bucket {rank}")
            save_to_bucket(message['key'], message['value'])
        elif message['action'] == 'get':
            value = get_from_bucket(message['key'])
            comm.send({'action': 'get_result', 'key': message['key'], 'value': value}, dest=0)
        elif message['action'] == 'find':
            found = find_in_bucket(message['key'])
            comm.send({'action': 'find_result', 'key': message['key'], 'found': found}, dest=0)
        elif message['action'] == 'set':
            found_and_updated = set_in_bucket(message['key'], message['value'])
            comm.send({'action': 'set_result', 'success': found_and_updated}, dest=0)
        elif message['action'] == 'delete':
            found_and_deleted = delete_from_bucket(message['key'])
            comm.send({'action': 'delete_result', 'success': found_and_deleted}, dest=0)
        elif message['action'] == 'check_balance':
            total_lines = 0
            with open(f"bucket_{rank}.txt", "r") as file:
                total_lines = len(file.readlines())
            comm.send({'action': 'balance', 'total_lines': total_lines, 'rank': rank}, dest=0)
        elif message['action'] == 'donate':
            last_line = None
            with open(f"bucket_{rank}.txt", "r+") as file:
                lines = file.readlines()
                if lines:
                    last_line = lines[-1]
                    file.seek(0)
                    for line in lines[:-1]:
                        file.write(line)
                    file.truncate()
                else:
                    print("File is empty, no line to delete")

            if last_line:
                key, value = last_line.strip().split(':')
                comm.send({'action': 'insert_from_siblings', 'key': key, 'value': value, 'destination': message['receiver']}, dest=message['receiver'])

        # Handle messages from other siblings
        if status.Get_source() != 0:
            if message['action'] == 'insert_from_siblings' and message['destination'] == rank:
                print(f"[REBALANCE] Receiving data {message['value']} from bucket {status.Get_source()}")
                save_to_bucket(message['key'], message['value'])

comm.Barrier()