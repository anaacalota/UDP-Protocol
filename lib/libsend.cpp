#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int configured_speed = 0;
int configured_delay = 0;

void send_one(struct connection *con, send_segment &segment)
{
    // build the protocol header on top of the payload and send a single UDP segment
    poli_tcp_data_hdr hdr = {
        .protocol_id = POLI_PROTOCOL_ID,
        .conn_id = (uint8_t)con->conn_id,
        .type = TYPE_DATA,
        .seq_num = htons(segment.seq),
        .len = htons((uint16_t)segment.payload.size())
    };

    char packet[MAX_SEGMENT_SIZE];
    char *pointer = packet;
    memcpy(pointer, &hdr, sizeof(hdr));
    pointer += sizeof(hdr);
    memcpy(pointer, segment.payload.data(), segment.payload.size());
    pointer += segment.payload.size();
    sendto(con->sockfd, packet, pointer - packet, 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
    segment.sent = true;
}

void transmit_window(struct connection *con)
{
    sender_state *send_state = con->send_state;
    if (send_state == NULL) {
        return;
    }

    // move data from the queue into the transmission window while there is room and data left to send
    while (!send_state->queued.empty() && (int)send_state->no_ack.size() < send_state->max_window) {
        int bytes_to_send = min(MAX_DATA_SIZE, (int)send_state->queued.size());
        send_segment segment;
        segment.sent = false;
        segment.seq = send_state->next_seq++;

        // build the segment payload from the queue
        for (int i = 0; i < bytes_to_send; i++) {
            segment.payload.push_back(send_state->queued.front());
            send_state->queued.pop_front();
        }

        send_state->no_ack.push_back(segment);
        send_one(con, send_state->no_ack.back());
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    sender_state *send_state = cons[conn_id]->send_state;
    if (send_state == NULL) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }

    // compute how many bytes are already used in the buffer
    int used_count = (int)send_state->queued.size(); // bytes in the queue
    for (int i = 0; i < (int)send_state->no_ack.size(); i++) {
        used_count += (int)send_state->no_ack[i].payload.size(); // bytes from unacknowledged segments
    }
    size = min(len, send_state->max_buffer - used_count);
    if (size <= 0) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }
    // add the new data to the queue
    for (int i = 0; i < size; i++) {
        send_state->queued.push_back(buffer[i]);
    }
    // send as many new segments as fit in the window
    transmit_window(cons[conn_id]);
    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void retransmit_window(struct connection *con)
{
    sender_state *send_state = con->send_state;
    if (send_state == NULL) {
        return;
    }
    // retransmit all segments that have not been ACKed
    for (int i = 0; i < (int)send_state->no_ack.size(); i++) {
        if (send_state->no_ack[i].sent) {
            send_one(con, send_state->no_ack[i]);
        }
    }
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        // if there are no active connections, wait a little to avoid wasting CPU
        if (cons.size() == 0) {
            usleep(1000);
            continue;
        }
        int conn_id = -1;
        res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        if (res != -14 && conn_id != -1 && cons.find(conn_id) != cons.end()) {
            pthread_mutex_lock(&cons[conn_id]->con_lock);
            sender_state *send_state = cons[conn_id]->send_state;

            if (send_state) {
                // if the timer expired, retransmit the unacknowledged segments
                if (res == -1) {
                    retransmit_window(cons[conn_id]);
                } else if (res >= (int)sizeof(poli_tcp_data_hdr)) {
                    poli_tcp_ctrl_hdr ack;
                    memcpy(&ack, buf, sizeof(poli_tcp_ctrl_hdr));
                    if (ack.protocol_id == POLI_PROTOCOL_ID && ack.type == TYPE_ACK) {
                        uint16_t ack_nr = ntohs(ack.ack_num);
                        // remove the acknowledged segments from the window
                        while (!send_state->no_ack.empty() && send_state->no_ack.front().seq - ack_nr < 0) {
                            send_state->no_ack.pop_front();
                        }
                        // try to send new segments that fit in the window after receiving the ACK
                        transmit_window(cons[conn_id]);
                    }
                }
            }
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
        }

        // send new segments for all active connections that have data to send
        // and that have room in the transmission window
        for (auto i = cons.begin(); i != cons.end(); ++i) {
            connection *con = i->second;
            pthread_mutex_lock(&con->con_lock);
            transmit_window(con);
            pthread_mutex_unlock(&con->con_lock);
        }

        if (res == -14) {
            usleep(500);
        }
    }

    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    if (con == NULL)
        return -1;
    memset(con, 0, sizeof(*con));

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (con->sockfd < 0) {
        free(con);
        return -1;
    }

    con->servaddr = {
        .sin_family = AF_INET,
        .sin_port = port,
        .sin_addr = { .s_addr = ip }
    };

    // build the SYN segment to initiate the connection
    poli_tcp_ctrl_hdr syn = {
        .protocol_id = POLI_PROTOCOL_ID,
        .type = TYPE_SYN
    };

    // set a timeout so recvfrom does not block forever
    struct timeval tv = {
        .tv_sec = 0,
        .tv_usec = 200000
    };
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char recv_buf[MAX_SEGMENT_SIZE];
    uint16_t data_port = 0;
    int conn_id = 0;

    // send the SYN and wait for the SYN-ACK
    for (int attempt = 0; attempt < 100 && data_port == 0; attempt++) {
        sendto(con->sockfd, &syn, sizeof(syn), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        struct sockaddr_in reply_addr;
        socklen_t reply_len = sizeof(reply_addr);
        int n = recvfrom(con->sockfd, recv_buf, sizeof(recv_buf), 0, (struct sockaddr *)&reply_addr, &reply_len);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr)) {
            continue;
        }
        poli_tcp_ctrl_hdr synack{};
        memcpy(&synack, recv_buf, sizeof(synack));
        // if a valid SYN-ACK was received, extract the data port from it
        bool valid_synack = synack.protocol_id == POLI_PROTOCOL_ID && synack.type == TYPE_SYN_ACK;
        if (!valid_synack) {
            continue;
        }
        conn_id = synack.conn_id;
        data_port = ntohs(synack.recv_window);
    }
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // if no SYN-ACK was received, the connection failed
    if (data_port == 0) {
        close(con->sockfd);
        free(con);
        return -1;
    }

    con->conn_id = conn_id;
    con->servaddr.sin_port = htons(data_port);
    // send the ACK that completes the handshake
    poli_tcp_ctrl_hdr ack = {
        .protocol_id = POLI_PROTOCOL_ID,
        .conn_id = (uint8_t)conn_id,
        .type = TYPE_ACK
    };
    sendto(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    // initialize the sender state for the new connection
    sender_state *send_state = new sender_state();
    send_state->next_seq = 1;
    // window size derived from the bandwidth-delay product (BDP)
    long long bdp_bytes = (long long)configured_speed * 1000000LL * max(1, configured_delay * 2) / 1000 / 8;
    send_state->max_window = max(8, min(64, (int)(bdp_bytes / MAX_DATA_SIZE) + 4));
    send_state->max_buffer = 16 * 1024 * 1024;
    con->send_state = send_state;

    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    int rto_ms = TIMEOUT_SEND(configured_delay);
    spec.it_value.tv_sec = rto_ms / 1000;    
    spec.it_value.tv_nsec = (rto_ms % 1000) * 1000000;    
    spec.it_interval.tv_sec = rto_ms / 1000;    
    spec.it_interval.tv_nsec = (rto_ms % 1000) * 1000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    configured_speed = speed;
    configured_delay = delay;

    int ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
    pthread_detach(thread1);
}
