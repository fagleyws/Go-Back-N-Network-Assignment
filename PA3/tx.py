import socket
import argparse
from PA3.packet import Packet
from PA3.cQueue import CircularQueue
import logging

PAYLOAD_SIZE = 50
SEQNUM_SIZE = 10
WINDOW_SIZE = 5
logging.basicConfig(filename="Tx.log",level=logging.DEBUG)
packetList = list()


def reliablyTransfer(rx_ip, rx_port, filename):
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mySocket.settimeout(5)  # TIMEOUT

    window = CircularQueue(WINDOW_SIZE)

    base = 0          # oldest unACKed packet
    next_seq = 0      # next sequence number to use

    packets = []

    # Read file and create all packets first
    with open(filename, "rb") as file:
        while chunk := file.read(PAYLOAD_SIZE):
            packets.append(Packet(1, next_seq % SEQNUM_SIZE, len(chunk), chunk.decode()))
            next_seq += 1

    total_packets = len(packets)

    base = 0
    next_seq = 0

    while base < total_packets:

        # Fill window
        while not window.isFull() and next_seq < total_packets:
            pkt = packets[next_seq]
            mySocket.sendto(pkt.serialize(), (rx_ip, rx_port))
            logging.debug(f"Sent packet {pkt.seqnum}")

            window.enqueue(pkt)
            next_seq += 1

        try:
            # Receive ACK
            ack_data, _ = mySocket.recvfrom(1024)
            ack_packet = Packet.deserialize(ack_data)

            if ack_packet.flag == 0:
                ack_num = ack_packet.seqnum
                logging.debug(f"Received ACK {ack_num}")

                # Slide window (cumulative ACK)
                while not window.isEmpty():
                    front_pkt = window.getFront()
                    if front_pkt.seqnum <= ack_num:
                        window.dequeue()
                        base += 1
                    else:
                        break
            if ack_packet.flag == 2:
                ack_num = ack_packet.seqnum
                logging.debug(f"Received FINACK {ack_num}")
        except socket.timeout:
            # TIMEOUT → Go-Back-N resend
            logging.debug("Timeout! Resending window")

            temp_list = []
            while not window.isEmpty():
                pkt = window.dequeue()
                temp_list.append(pkt)

            # re-enqueue + resend
            for pkt in temp_list:
                mySocket.sendto(pkt.serialize(), (rx_ip, rx_port))
                logging.debug(f"Resent packet {pkt.seqnum}")
                window.enqueue(pkt)

    # Send FIN
    fin = Packet(2, 0, 0, "")
    mySocket.sendto(fin.serialize(), (rx_ip, rx_port))

    # Wait for FIN ACK
    while True:
        try:
            data, _ = mySocket.recvfrom(1024)
            pkt = Packet.deserialize(data)
            if pkt.flag == 0:
                break
        except socket.timeout:
            mySocket.sendto(fin.serialize(), (rx_ip, rx_port))

    mySocket.close()
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UDP Transmitter")
    parser.add_argument("-ip", metavar="IP address", default="127.0.0.1", type=str, help="Receiver IP address")
    parser.add_argument("-p", metavar="Port number", default=12345, type=int, help="Receiver port number")
    parser.add_argument("-f", metavar="File path", default="data/small.txt", type=str, help="Path to the file to send")

    args = parser.parse_args()

    reliablyTransfer(args.ip, args.p, args.f)
