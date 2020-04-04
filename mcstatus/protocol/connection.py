import asyncio
import socket
import struct


class Connection:
    def __init__(self):
        self.sent = bytearray()
        self.received = bytearray()

    async def read(self, length):
        result = self.received[:length]
        self.received = self.received[length:]
        return result

    def write(self, data):
        if isinstance(data, Connection):
            data = bytearray(data.flush())
        if isinstance(data, str):
            data = bytearray(data)
        self.sent.extend(data)

    def receive(self, data):
        if not isinstance(data, bytearray):
            data = bytearray(data)
        self.received.extend(data)

    def remaining(self):
        return len(self.received)

    def flush(self):
        result = self.sent
        self.sent = ""
        return result

    def _unpack(self, format, data):
        return struct.unpack(">" + format, bytes(data))[0]

    def _pack(self, format, data):
        return struct.pack(">" + format, data)

    async def read_varint(self):
        result = 0
        for i in range(5):
            part = ord(await self.read(1))
            result |= (part & 0x7F) << 7 * i
            if not part & 0x80:
                return result
        raise IOError("Server sent a varint that was too big!")

    def write_varint(self, value):
        remaining = value
        for i in range(5):
            if remaining & ~0x7F == 0:
                self.write(struct.pack("!B", remaining))
                return
            self.write(struct.pack("!B", remaining & 0x7F | 0x80))
            remaining >>= 7
        raise ValueError("The value %d is too big to send in a varint" % value)

    async def read_utf(self):
        length = await self.read_varint()
        return (await self.read(length)).decode('utf8')

    def write_utf(self, value):
        self.write_varint(len(value))
        self.write(bytearray(value, 'utf8'))

    async def read_ascii(self):
        result = bytearray()
        while len(result) == 0 or result[-1] != 0:
            result.extend(await self.read(1))
        return result[:-1].decode("ISO-8859-1")

    def write_ascii(self, value):
        self.write(bytearray(value, 'ISO-8859-1'))
        self.write(bytearray.fromhex("00"))

    async def read_short(self):
        return self._unpack("h", await self.read(2))

    def write_short(self, value):
        self.write(self._pack("h", value))

    async def read_ushort(self):
        return self._unpack("H", await self.read(2))

    def write_ushort(self, value):
        self.write(self._pack("H", value))

    async def read_int(self):
        return self._unpack("i", await self.read(4))

    def write_int(self, value):
        self.write(self._pack("i", value))

    async def read_uint(self):
        return self._unpack("I", await self.read(4))

    def write_uint(self, value):
        self.write(self._pack("I", value))

    async def read_long(self):
        return self._unpack("q", await self.read(8))

    def write_long(self, value):
        self.write(self._pack("q", value))

    async def read_ulong(self):
        return self._unpack("Q", await self.read(8))

    def write_ulong(self, value):
        self.write(self._pack("Q", value))

    async def read_buffer(self):
        length = await self.read_varint()
        result = Connection()
        result.receive(await self.read(length))
        return result

    def write_buffer(self, buffer):
        data = buffer.flush()
        self.write_varint(len(data))
        self.write(data)


class TCPSocketConnection(Connection):
    def __init__(self, addr, timeout=3):
        Connection.__init__(self)

        self.host = addr[0]
        self.port = addr[1]
        self.timeout = timeout

        self._reader = None
        self._writer = None

    async def __aenter__(self):
        if not self._writer:
            fut = asyncio.open_connection(self.host, self.port)
            self._reader, self._writer = await asyncio.wait_for(fut, timeout=self.timeout)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._writer:
            self._writer.close()

    def flush(self):
        raise TypeError("TCPSocketConnection does not support flush()")

    def receive(self, data):
        raise TypeError("TCPSocketConnection does not support receive()")

    def remaining(self):
        raise TypeError("TCPSocketConnection does not support remaining()")

    async def read(self, length):
        result = bytearray()
        while len(result) < length:
            new = await self._reader.read(length - len(result))
            if len(new) == 0:
                raise IOError("Server did not respond with any information!")
            result.extend(new)
        return result

    def write(self, data):
        self._writer.write(data)


class UDPSocketConnection(Connection):
    def __init__(self, addr, timeout=3):
        Connection.__init__(self)
        self.addr = addr
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)

    def flush(self):
        raise TypeError("UDPSocketConnection does not support flush()")

    def receive(self, data):
        raise TypeError("UDPSocketConnection does not support receive()")

    def remaining(self):
        return 65535

    def read(self, length):
        result = bytearray()
        while len(result) == 0:
            result.extend(self.socket.recvfrom(self.remaining())[0])
        return result

    def write(self, data):
        if isinstance(data, Connection):
            data = bytearray(data.flush())
        self.socket.sendto(data, self.addr)
