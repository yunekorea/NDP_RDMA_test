import selectors
import socket
import sys
import os
import pprint
import mmap
import struct

import ctypes
from ctypes.util import find_library

from pyverbs.device import Context
from pyverbs.pd import PD
from pyverbs.mr import MR
from pyverbs.libibverbs_enums import ibv_access_flags as fe


dev_name = "mlx5_0".encode('utf-8')
dev_name_len = len(dev_name)
# Initialize RDMA Device
ctx = Context(name='mlx5_0')
pd = PD(ctx)
host_ip = "192.168.100.2"
sel = selectors.DefaultSelector()

def read_metadata(conn, mask):
    data = conn.recv(128)
    if data:
        print("Interrupt received! Processing metadata...")
        # Process your RDMA logic here
        struct_format = "<QIIQ50s" 
        
        try:
            # We slice the data to match the expected struct size
            unpacked = struct.unpack(struct_format, data[:struct.calcsize(struct_format)])
            
            addr        = unpacked[0]
            rkey        = unpacked[1]
            length      = unpacked[2]
            name_length = unpacked[3]
            # Decode the name and strip null bytes (\x00)
            device_name = unpacked[4][:name_length].decode('utf-8').strip('\x00')
            
            print(f"--- Decoded Metadata ---")
            print(f"Address:     {hex(addr)}")
            print(f"R-Key:       {hex(rkey)}")
            print(f"Length:      {length}")
            print(f"Name Length: {name_length}")
            print(f"Device Name: {device_name}")
            
            # Now you can proceed with your RDMA logic using these variables
        local_mr = MR(pd, length, ibv_access_flags.IBV_ACCESS_LOCAL_WRITE)

            
        except struct.error as e:
            print(f"Error unpacking metadata: {e}")
        
    sel.unregister(conn)
    conn.close()


def accept_connection(sock, mask):
    conn, _ = sock.accept()
    conn.setblocking(False)
    # Register the 'read' event for this specific connection
    sel.register(conn, selectors.EVENT_READ, read_metadata)

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind('/tmp/rdma_metadata.sock')
server.listen(1)
server.setblocking(False)

# Register the main socket for the 'accept' event
sel.register(server, selectors.EVENT_READ, accept_connection)

print("Python is ready. Waiting for asynchronous events...")
while True:
    events = sel.select() # This blocks efficiently (uses epoll/kqueue)
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)

