
import struct

# Delimiter
delimiter = "|*|*|"


class Packet:

    def __init__(self, flag, job_id, client_id, seq, index, msg, buf):
        self.flag = flag
        self.job_id = job_id
        self.client_id = client_id
        self.seq = seq
        self.index = index
        self.msg = msg
        # Buffer of encoding
        self.buf = buf

    # Encode
    def encode_buf(self):
        self.buf = struct.pack("!IIIIi100s", self.flag, self.job_id, self.client_id, self.seq, self.index, str.encode(str(self.msg).ljust(100,' ')))

    # Decode
    def decode_buf(self):
        self.flag, self.job_id, self.client_id, self.seq, self.index, self.msg = struct.unpack("!IIIIi100s", self.buf)
        self.msg = self.msg.decode()
