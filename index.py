import os
import uuid
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
next_bucket = 1

def save_last_key(key):
    with open("last_key.txt", "w") as file:
        file.write(str(key))

def get_last_key():
    try:
        with open("last_key.txt", "r") as file:
            return int(file.read())
    except FileNotFoundError:
        return None

def insert_data(value):
    global next_bucket
    key = get_last_key() + 1 if get_last_key() else 1
    comm.send({'action': 'insert', 'key': key, 'value': value}, dest=next_bucket)
    next_bucket = (next_bucket % (size - 1)) + 1
    save_last_key(key)

def rebalance_insert_data(key, value):
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

def update_data(key, value):
    for bucket in range(1, size):
        comm.send({'action': 'update', 'key': key, 'value': value}, dest=bucket)

def find_data(key):
    for bucket in range(1, size):
        comm.send({'action': 'find', 'key': key}, dest=bucket)

def load_last_key():
    try:
        with open("last_key.txt", "r") as file:
            return file.read()
    except FileNotFoundError:
        return None

def find_in_bucket(key):
    found = False

    with open(f"bucket_{rank}.txt", "r") as file:
        for line in file:
            id, value = line.strip().split(':')
            if key == id:
                found = True
                break

    return found

def get_from_bucket(key):
    value = None

    with open(f"bucket_{rank}.txt", "r") as file:
        for line in file:
            id, val = line.strip().split(':')
            if key == id:
                value = val
                break

    return value

def delete_from_bucket(key):
    with open(f"bucket_{rank}.txt", "r+") as file:
        lines = file.readlines()
        file.seek(0)
        for line in lines:
            id, value = line.strip().split(':')
            if key != id:
                file.write(line)
        file.truncate()

def update_in_bucket(key, new_value):
    with open(f"bucket_{rank}.txt", "r+") as file:
        lines = file.readlines()
        file.seek(0)
        for line in lines:
            id, value = line.strip().split(':')
            if key == id:
                file.write(f"{id}:{new_value}\n")
            else:
                file.write(line)
        file.truncate()

def create_buckets():
    for i in range(1, size):
        if not os.path.exists(f"bucket_{i}.txt"):
            open(f"bucket_{i}.txt", "w").close()

def is_balanced():
    bucket_files = [f"bucket_{i}.txt" for i in range(1, size)]
    bucket_line_counts = {}
    total_lines = 0

    for bucket_file in bucket_files:
        try:
            with open(bucket_file) as file:
                line_count = len(file.readlines())
            bucket_line_counts[bucket_file] = line_count
            total_lines += line_count
        except FileNotFoundError:
            bucket_line_counts[bucket_file] = 0

    print(f"Total lines: {total_lines}")

    if total_lines < size:
        return True, bucket_line_counts

    average_lines = total_lines / len(bucket_line_counts)

    for bucket_file, line_count in bucket_line_counts.items():
        if line_count > 1.3 * average_lines or line_count < 0.7 * average_lines:
            return False, bucket_line_counts

    return True, bucket_line_counts

def rebalance_buckets(bucket_line_counts):
    all_data = []

    for bucket_file in bucket_line_counts.keys():
        try:
            with open(bucket_file, "r") as file:
                all_data.extend(file.readlines())
        except FileNotFoundError:
            pass

    if len(all_data) == 0:
        return

    for bucket_file in bucket_line_counts.keys():
        open(bucket_file, "w").close()

    print("Data before rebalance:")

    for bucket_file, line_count in bucket_line_counts.items():
        print(f"Bucket {bucket_file} has {line_count} lines.")

    for data in all_data:
        key, value = data.strip().split(':')
        rebalance_insert_data(key, value)

def save_to_bucket(key, value):
    with open(f"bucket_{rank}.txt", "a") as file:
        file.write(f"{key}:{value}\n")

if rank == 0:
    create_buckets()

    for _ in range(10000):
        insert_data(str(uuid.uuid4()))

    get_data("1")

    update_data("1", "new_value")

    get_data("1")

    find_data("2")

    delete_data("2")

    find_data("2")

    balanced, bucket_line_counts = is_balanced()

    print(f"Is the data balanced? {balanced}")

    while not balanced:
        rebalance_buckets(bucket_line_counts)
        balanced, bucket_line_counts = is_balanced()
        print(f"Is the data balanced? {balanced}")

    for bucket_file, line_count in bucket_line_counts.items():
        print(f"Bucket {bucket_file} has {line_count} lines.")

    for i in range(1, size):
        comm.send({'action': 'stop'}, dest=i)
else:
    while True:
        message = comm.recv(source=0)

        if message['action'] == 'stop':
            break
        elif message['action'] == 'insert':
            save_to_bucket(message['key'], message['value'])
        elif message['action'] == 'delete':
            delete_from_bucket(message['key'])
        elif message['action'] == 'update':
            update_in_bucket(message['key'], message['value'])
        elif message['action'] == 'get':
            result = get_from_bucket(message['key'])
            if result:
                print(f"Data for key {message['key']} is: {result}")
        elif message['action'] == 'find':
            found = find_in_bucket(message['key'])
            if found:
                print(f"Key {message['key']} found in bucket {rank}")
                
comm.Barrier()