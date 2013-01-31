import datetime
import socket

import beaver.transport


class UdpTransport(beaver.transport.Transport):

    def __init__(self, beaver_config, file_config, logger=None):
        super(UdpTransport, self).__init__(beaver_config, file_config, logger=logger)

        self._sock = socket.socket(socket.AF_INET,  # Internet
            socket.SOCK_DGRAM)  # UDP
        self._address = (beaver_config.get('udp_host'), int(beaver_config.get('udp_port')))

    def callback(self, filename, lines):
        for line in lines:
            formatted = self.format(filename, line)
            self._sock.sendto(formatted, self._address)
