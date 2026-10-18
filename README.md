Copyright 2026 Ana-Cristina Calota

# Reliable UDP Protocol

## Overview
The project represents the implementation of a reliable transport protocol on
top of UDP, written in C++. It transfers a stream of bytes between a client
(sender) and a server (receiver) and provides logical connections, segmented
data transfer, acknowledgements, retransmission on timeout and sliding windows
on both the sender and the receiver side.

## Project Details
The protocol uses two packed headers defined in protocol.h: poli_tcp_data_hdr
for data segments and poli_tcp_ctrl_hdr for control segments. Both of them own a
type field that identifies the segment:
- TYPE_SYN = 1: request to start a connection
- TYPE_SYN_ACK = 2: answer of the receiver to a SYN
- TYPE_ACK = 3: acknowledgement of the received data
- TYPE_DATA = 4: segment that carries data

The maximum payload of a segment is 512 bytes (MAX_DATA_SIZE), and the maximum
size of a UDP segment sent over the network is MAX_SEGMENT_SIZE (payload plus
header).

The library is split into three files:
- libsend.cpp: the sender side (connection setup, send_data, sending and
  retransmitting the window).
- librecv.cpp: the receiver side (wait4connect, recv_data, processing of the
  received segments, sending the acks).
- libcommon.cpp: recv_message_or_timeout, the function used by both sides to
  find out if a socket has data or if a timer has expired.

Every connection is described by the connection structure, which stores its
socket, its id, the address of the other side, a mutex and either the state of
the sender (sender_state) or the state of the receiver (recv_buffer_state). The
connections are stored in a map indexed by conn_id, and each of them has its own
UDP socket, bound to a random free port chosen during the handshake. The socket
and the timer of every connection are also kept in the poll arrays used by
recv_message_or_timeout.

A background thread (sender_handler or receiver_handler) processes the incoming
segments and the timers, and uses the mutex of the connection to avoid
synchronization problems with the calls made by the application.

## Connection setup
The connection is established using a three way handshake:
1. The sender sends a SYN segment to the listen port of the server (8032).
2. The receiver waits for a SYN on the listen socket, creates a new UDP socket
   for that connection and binds it to a free port. Then it sends back a
   SYN-ACK, in which the recv_window field is used to carry the chosen data
   port.
3. The sender receives the SYN-ACK, updates the address of the destination with
   the received data port and sends a final ACK.

The sender uses a timeout on the socket during the handshake, so it can
retransmit the SYN if it receives no answer (up to 100 attempts, 200 ms each).
The receiver sends the SYN-ACK several times to reduce the chances of the
handshake failing because of a lost packet.

## Sending data
The send_data function copies the data received from the application into the
queued buffer, but only if there is enough space in the buffer of the
sender (16 MB). If there is no space, it returns -1.

Then, transmit_window splits the queued data into segments and sends them as
long as the number of unacknowledged segments is smaller than max_window. The
size of the window is computed from the bandwidth-delay product (BDP), based on
the speed and the delay received in init_sender, and it is kept between 8 and 64
segments.

The segments that were sent but not acknowledged yet are kept in the no_ack
queue.

## Acknowledgements and retransmission
When the sender receives a valid ACK, it removes from no_ack all the segments
with a sequence number smaller than ack_num, then it tries to send the new
segments that became eligible in the window.

Every connection has a timer created with timerfd_create. When the timer
expires, the sender retransmits all the segments that are still in no_ack. The
timeout is computed from the configured delay (TIMEOUT_SEND). On the receiver
side, an expired timer makes the receiver resend its current ack.

## Receiving data
When a segment is received:
- if seq == next_expected_seq: the segment goes into ready, then the receiver
  tries to promote the consecutive segments from out_of_order.
- if seq > next_expected_seq: the segment is kept in out_of_order, if it fits in
  the buffer and it is not a duplicate (it arrived before other packets that are
  still on the network).
- if seq < next_expected_seq: the segment is a duplicate and it is ignored (it
  was already processed and added to ready).

The recv_data function waits until there is data in ready, then copies in the
buffer of the application at most the number of bytes requested. The data is
delivered only in order, and after every read an ack with the new free space of
the buffer is sent.

## Available functions
wait4connect: on the receiver - blocks until a client sends a SYN, then creates
the connection and returns its id.

setup_connection: on the sender - performs the handshake and returns the id of
the connection, or -1 if it failed.

send_data: on the sender - adds bytes to the send buffer and transmits the
segments that fit in the window.

recv_data: on the receiver - blocks until data is available and copies it to the
buffer of the application.

init_sender: starts the sender handler thread and stores the speed and the delay
of the link.

init_receiver: creates the listen socket, stores the size of the receiver buffer
and starts the receiver handler thread.

recv_message_or_timeout: checks if a socket has data or if a timer has expired,
and returns the id of the connection it happened on.
