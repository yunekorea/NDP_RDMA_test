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

libc.free(bufferptr)

print(result)
