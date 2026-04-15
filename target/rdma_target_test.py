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
from pyverbs.cmid import CMID, AddrInfo
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.libibverbs_enums import ibv_access_flags, ibv_qp_type, ibv_wr_opcode
from pyverbs.librdmacm_enums import rdma_port_space
import pyverbs.wr as pwr


dev_name = "mlx5_0".encode('utf-8')
dev_name_len = len(dev_name)
# Initialize RDMA Device
ctx = Context(name='mlx5_0')
pd = PD(ctx)
host_ip = "192.168.100.2"
sel = selectors.DefaultSelector()
cai = AddrInfo(dst=host_ip, dst_service="7471",
                port_space = rdma_port_space.RDMA_PS_TCP)
cap = QPCap(max_send_wr=5, max_recv_wr=5, max_send_sge=1)
qp_init_attr = QPInitAttr(cap=cap, qp_type=ibv_qp_type.IBV_QPT_RC)
print(f"Connecting to Host at {host_ip}...")
cid = CMID(creator=cai, qp_init_attr=qp_init_attr)

def read_metadata(conn, mask):
    data = conn.recv(128)
    if data:
        print("Interrupt received! Processing metadata...")
        # Process your RDMA logic here
        struct_format = "<QQII50s" 
        
        try:
            # We slice the data to match the expected struct size
            unpacked = struct.unpack(struct_format, data[:struct.calcsize(struct_format)])
            
            rkey        = unpacked[0]
            addr        = unpacked[1]
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

            cid.connect()

            #wr = pwr.SendWR(opcode=ibv_wr_opcode.IBV_WR_RDMA_READ, num_sge=1)
            #wr.set_sgl(local_mr)

            # Create SGE explicitly
            sge = pwr.SGE(
                addr=local_mr.buf,
                length=length,
                lkey=local_mr.lkey
            )

            # Attach SGE to WR
            wr = pwr.SendWR(
                opcode=ibv_wr_opcode.IBV_WR_RDMA_READ,
                num_sge = 1
            )
            wr.set_sgl(sge)

            # Set the remote memory details
            wr.wr.rdma.remote_addr = addr
            wr.wr.rdma.rkey = rkey            

            cid.qp.post_send(wr)

            wc = cid.cq.poll()[0]
            
            if wc.status == 0: # Success
                print("RDMA Read Successful!")
                # Verify by reading the local buffer content
                # mr.read(length, offset) returns the data
                print(f"Data from Host: {local_mr.read(length, 0)}")
            else:
                print(f"RDMA Read Failed. Status code: {wc.status}")
        
        except struct.error as e:
            print(f"Error unpacking metadata: {e}")
        except Exception as e:
            print(f"RDMA Operation error: {e}")

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
try:
    while True:
        events = sel.select() # This blocks efficiently (uses epoll/kqueue)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)
except KeyboardInterrupt:
    print("Shutting down...")
finally:
    cid.close()

