"""
Milter example with tornado framework (http://www.tornadoweb.org/)
"""

import time
from signal import SIGINT, SIGTERM, signal
import struct
import logging

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado.gen import coroutine
from tornado.iostream import StreamClosedError

from ppymilterbase import PpyMilterDispatcher, PpyMilter, PpyMilterCloseConnection

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 10
PORT = 9999
MILTER_LEN_BYTES = 4  # from sendmail's include/libmilter/mfdef.h

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d@%H:%M:%S')
logger = logging.getLogger('ppymilter')


class MilterClient(object):
    """
    MilterClient represents a milter client to take care about
    """
    def __init__(self, stream, address, milter_class, context):
        self.stream = stream
        self.address = address
        self.client_host = None
        self.milter_dispatcher = PpyMilterDispatcher(milter_class, context)

    @coroutine
    def on_connect(self):
        try:
            server_sockname = self.stream.socket.getsockname()
            if len(server_sockname) == 2:
                (self.client_host, client_port) = self.stream.socket.getpeername()                      # ipv4
            elif len(server_sockname) == 4:
                (self.client_host, client_port, flowinfo, scopeid) = self.stream.socket.getpeername()   # ipv6
            else:
                return
        except StreamClosedError:
            # The client went away before it we could do anything with it
            self.disconnect()
            return

        yield self.handle()

    @coroutine
    def handle(self):
        try:
            while True:
                packetlen = yield self.stream.read_bytes(MILTER_LEN_BYTES)
                packetlen = int(struct.unpack('!I', packetlen)[0])
                data = yield self.stream.read_bytes(packetlen)
                try:
                    response = self.milter_dispatcher.Dispatch(data)
                    if isinstance(response, list):
                        for r in response:
                            yield self.send_response(r)
                    elif response:
                        yield self.send_response(response)
                except PpyMilterCloseConnection as ex:
                    logger.info('Closing connection ("%s")', str(ex))
                    self.disconnect()

        except StreamClosedError:
            self.disconnect()

    @coroutine
    def send_response(self, response):
        length = struct.pack('!I', len(response))
        yield self.stream.write(length + response)

    def disconnect(self):
        self.stream.close()


class TornadoMilterServer(TCPServer):
    """
    The milter TCP server
    """
    def __init__(self, milter_class, context):
        TCPServer.__init__(self)
        self.milter_class = milter_class
        self.context = context

    @coroutine
    def handle_stream(self, stream, address):
        """
        handle_stream is called by Tornado when we have a new client
        """
        client = MilterClient(
            stream=stream,
            address=address,
            milter_class=self.milter_class,
            context=self.context
        )
        yield client.on_connect()


class TornadoMilterExample(object):
    """
    Example implementation of a milter with Tornado frawework
    """
    def __init__(self, milter_class, context=None):
        self.server = None
        self.milter_class = milter_class
        self.context = context

    def main(self):
        signal(SIGTERM, self.sig_handler)
        signal(SIGINT, self.sig_handler)
        IOLoop.instance().add_callback(self.start)
        IOLoop.instance().start()

    def start(self):
        self.server = TornadoMilterServer(self.milter_class, self.context)
        self.server.listen(PORT)

    def sig_handler(self, sig, frame):
        """
        Signal handler, so that we can stop the server
        """
        IOLoop.instance().add_callback_from_signal(self.shutdown)

    def shutdown(self):
        """
        Cleanly shutdowns the Tornado loop
        """
        self.server.stop()
        io_loop = IOLoop.instance()
        deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
        countdown = MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

        def stop_loop(counter):
            now = time.time()
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.call_later(1, stop_loop, counter - 1)
            else:
                io_loop.stop()

        stop_loop(countdown)


class DummyMilter(PpyMilter):
    """
    Dummy milter that just prints the commands sent by the client
    """

    def __init__(self, context=None):
        PpyMilter.__init__(self)
        self.context = context
        self.mutations = list()

    def OnConnect(self, cmd, hostname, family, port, address):
        print "connect from '{}'".format(hostname)
        return self.Continue()

    def OnHelo(self, cmd, helo_hostname):
        print "helo '{}'".format(helo_hostname)
        return self.Continue()

    def OnMailFrom(self, cmd, mailfrom, esmtp_info):
        print "mail from '{}'".format(mailfrom)
        return self.Continue()

    def OnRcptTo(self, cmd, rcptto, esmtp_info):
        print "rcpt to '{}'".format(rcptto)
        return self.Continue()

    def OnHeader(self, cmd, key, val):
        print "header: '{}' = '{}'".format(key, val)
        return self.Continue()

    def OnEndHeaders(self, cmd):
        print "end headers"
        return self.Continue()

    def OnBody(self, cmd, data):
        print "body chunk"
        return self.Continue()

    def OnResetState(self):
        self.mutations = list()

    def OnEndBody(self, cmd):
        print "end body"
        tmp = self.mutations
        self.mutations = list()
        return self.ReturnOnEndBodyActions(tmp)



if __name__ == '__main__':
    TornadoMilterExample(PpyMilter).main()
