import sys
import os
import pprint
import mmap
import struct

from libnvme import nvme

import ctypes
from ctypes.util import find_library

from pyverbs.device import Context
from pyverbs.pd import PD
from pyverbs.mr import MR
from pyverbs.libibverbs_enums import ibv_access_flags as fe


dev_name = "rocep59s0".encode('utf-8')
dev_name_len = len(dev_name)
# Initialize RDMA Device
ctx = Context(name='rocep59s0')
pd = PD(ctx)

buf_size = 4096 

flags = fe.IBV_ACCESS_LOCAL_WRITE | fe.IBV_ACCESS_REMOTE_WRITE | fe.IBV_ACCESS_REMOTE_READ

mr = MR(pd, buf_size, flags)

metadata = {
    "rkey": mr.rkey,
    "addr": mr.buf,
    "length": mr.length,
    "devnamelen": dev_name_len,
    "devname": dev_name
}

print(f"Metadata for NVMe-oF: Remote Key={hex(metadata['rkey'])}, Address={hex(metadata['addr'])}, \n\tLength={metadata['length']}, Name Len= {metadata['devnamelen']}, Name={metadata['devname']}")

#nvme setup

fd = nvme.nvme_open("nvme1n1")

cmd = nvme.ndp_passthru_cmd()
cmd.opcode = 0xdb
cmd.flags = 0
cmd.rsvd = 0
cmd.nsid = 1
cmd.cdw2 = 0
cmd.cdw3 = 0
cmd.cdw10 = 0
cmd.cdw11 = 0
cmd.cdw12 = 0
cmd.cdw13 = 0
cmd.cdw14 = 0
cmd.cdw15 = 0
cmd.data_len = 4096
cmd.data = 0
cmd.metadata_len = 0
cmd.metadata = 0
cmd.timeout_ms = 60000
cmd.result = 0

libc = ctypes.CDLL(find_library('c'))

bufferptr = ctypes.c_void_p()
buffersize = 4096 #Bytes

if libc.posix_memalign(ctypes.byref(bufferptr), libc.getpagesize(),
                       buffersize) != 0:
    raise Exception('ENOMEM')

ctypes.memset(bufferptr, 0, buffersize)
cmd.data = bufferptr.value

tempptr = bufferptr
pack_format = f"<QQII{dev_name_len}s"
packed_data = struct.pack(pack_format, metadata['rkey'], metadata['addr'], metadata['length'], metadata['devnamelen'], metadata['devname'])

ctypes.memmove(bufferptr.value, packed_data, len(packed_data))


print(cmd.data)
print(bufferptr)

result = nvme.ndp_passthru(fd, cmd)
print(result)

print("Starting RDMA listener to keep MR alive...")
try:
    # Setup listener on the same port the Target is looking for (7471)
    # AI_PASSIVE (0x1) allows it to bind to local addresses
    cai = AddrInfo(src_service="7471", port_space=rdma_port_space.RDMA_PS_TCP, flags=1)
    
    cap = QPCap(max_send_wr=5, max_recv_wr=5, max_send_sge=1)
    qp_init_attr = QPInitAttr(cap=cap, qp_type=ibv_qp_type.IBV_QPT_RC)
    
    # Listen for incoming connection
    listen_id = CMID(creator=cai, qp_init_attr=qp_init_attr)
    listen_id.listen()
    
    print("Waiting for Target to connect...")
    # This blocks until the Target calls cid.connect()
    conn_id = listen_id.accept()
    print("Target connected! The MR is now accessible.")

    # Stay alive as long as the connection exists
    input("Press Enter to disconnect and stop hosting memory (Target will lose access)...")

except Exception as e:
    print(f"RDMA Listener Error: {e}")

libc.free(bufferptr)
finally:
    # Cleanup
    libc.free(bufferptr)
    print("Process finished.")
