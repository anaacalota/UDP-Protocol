
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include <deque>
#include <vector>
#include <map>
#include <pthread.h>

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

// segment types used by the protocol
enum {
    TYPE_SYN = 1,
    TYPE_SYN_ACK = 2,
    TYPE_ACK = 3,
    TYPE_DATA = 4
};

// represents a segment that is waiting to be acknowledged
struct send_segment {
    bool sent; // true if it was sent at least once, false if it was never sent
    uint16_t seq;
    std::vector<char> payload;
};

struct sender_state {
    std::deque<char> queued; // data received from the application, not yet split into segments
    std::deque<send_segment> no_ack; // segments that were sent but not yet acknowledged
    uint16_t next_seq;
    int max_window;
    int max_buffer;
};

struct recv_buffer_state {
    std::vector<char> ready; // data already ordered, ready for recv_data
    std::map<uint16_t, std::vector<char>> out_of_order; // segments received, but with seq higher than next_expected_seq
    uint16_t next_expected_seq; // the next seq expected in order
    int max_buffer_size;
};

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    /* Parameters used only by the sender */
    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    sender_state *send_state;
    /* Parameters used only by the client */
    recv_buffer_state *recv_state;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
