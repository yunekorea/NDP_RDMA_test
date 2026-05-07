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
from pyverbs.addr import GlobalRoute
from pyverbs.addr import AH, AHAttr
from pyverbs.cmid import CMID, AddrInfo
from pyverbs.qp import QPInitAttr, QPCap, QPAttr, QP
from pyverbs.cq import CQ
from pyverbs.libibverbs_enums import ibv_access_flags, ibv_qp_type, ibv_wr_opcode
from pyverbs.librdmacm_enums import rdma_port_space, RAI_PASSIVE
import pyverbs.wr as pwr

import time

# Initialize RDMA Device
# 1. Open a device
dev_name = "mlx5_0".encode('utf-8')
dev_name_len = len(dev_name)
ctx = Context(name='mlx5_0')

# 2. Create PD
pd = PD(ctx)

# 3. Create CQ(Completion Queue)

num_cqes = 200 # can be adjusted
comp_vector = 63 # An arbitrary value. comp_vector is limited by the
                    # context's num_comp_vectors
cq = CQ(ctx, num_cqes, None, None, 0)
print(f"Completion Queue: {cq}")

# 4. Create QP(Queue Pair)
cap = QPCap(max_send_wr=16, max_recv_wr=16, max_send_sge=8)
#qp_init_attr = QPInitAttr(cap=cap, qp_type=ibv_qp_type.IBV_QPT_RC, scq=cq, rcq=cq)
qp_init_attr = QPInitAttr(cap=cap)
qp_attr = QPAttr()
print("qp_init_attr")
#qp = QP(pd, qp_init_attr, qp_attr)

# Initialize CIMD
host_ip = "192.168.100.2"
target_ip = "192.168.100.1"
cai = AddrInfo(src = target_ip,dst=host_ip, dst_service="9999",
                port_space = rdma_port_space.RDMA_PS_TCP)
cid = CMID(creator=cai, qp_init_attr=qp_init_attr)

qp_state = qp_attr.cur_qp_state
print(f"QP status BEFORE to_rts: {qp_state}")
# No need in case of RC. Needed for UD.
sel = selectors.DefaultSelector()

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
            print("cid.connect() start")
            cid.connect()
        
            local_mr = MR(pd, length, ibv_access_flags.IBV_ACCESS_LOCAL_WRITE)
            print("MR set")

            #cid.connect()
            #print("cid connected!")

            #wr = pwr.SendWR(opcode=ibv_wr_opcode.IBV_WR_RDMA_READ, num_sge=1)
            #wr.set_sgl(local_mr)

            # Create SGE explicitly
            sge = pwr.SGE(
                addr=local_mr.buf,
                length=length,
                lkey=local_mr.lkey
            )
            sgl = [sge]
            print("SGE set")

            # Attach SGE to WR
            wr = pwr.SendWR(opcode=ibv_wr_opcode.IBV_WR_RDMA_READ,
                num_sge = 1,
                sg = sgl
            )
            print("WR Set")
            #wr.set_sgl([sge])

            # Set the remote memory details
            #wr.wr.rdma.remote_addr = addr
            #wr.wr.rdma.rkey = rkey

            print(type(sge.addr))
            print(type(sge.lkey))
            print(type(addr))
            print(type(rkey))

            print(sge.addr)
            print(addr)

            wr.set_wr_rdma(int(addr), int(rkey))
            print("set_wr_rdma done")
            
            print("About to send WR")
            #cid.qp.post_send(wr)
            qp.post_send(wr, None)
            print("post send done")

            #wc = cid.cq.poll()[0]
            wc = cq.poll()[0]
            
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
#finally:
    #cid.close()

