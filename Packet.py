import hashlib
import base64
import struct

# Delimiter
delimiter = "|*|*|"


class Packet:

    def __init__(self, job_id, index, seq, offset, msg, buf):
        self.job_id = job_id
        self.index = index
        self.seq = seq
        self.offset = offset
        self.msg = msg
        # Buffer of encoding
        self.buf = buf

    # struct.pack("",,,,)
    # 包含payload的场景：
    # Encode
    def encode_seq(self):
        self.buf = struct.pack("!IIII100s", self.job_id, self.index, self.seq, self.offset, str.encode(str(self.msg).ljust(100,' ')))

    # Decode
    def decode_seq(self):
        self.job_id, self.index, self.seq, self.offset, self.msg = struct.unpack("!IIII100s", self.buf)


