#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include <vector>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
// listen socket bound to port 8032
int listen_sock = -1;
// size in bytes of the receiver buffer
int configured_recv_buffer;
int next_conn_id = 0;

// computes how many bytes are currently used in the buffer (both ready and out_of_order)
int buffered_bytes(recv_buffer_state *state)
{
    int bytes = state->ready.size();
    for (auto current = state->out_of_order.begin(); current != state->out_of_order.end(); ++current) {
        bytes += (int)current->second.size();
    }
    return bytes;
}

void send_ack(struct connection *con)
{
    recv_buffer_state *state = con->recv_state;
    // how much free space is left in the buffer
    int free_window = state->max_buffer_size - buffered_bytes(state);
    if (free_window < 0) {
        free_window = 0;
    }

    poli_tcp_ctrl_hdr ack = {
        .protocol_id = POLI_PROTOCOL_ID,
        .conn_id = (uint8_t)con->conn_id,
        .type = TYPE_ACK,
        // the ack carries the next expected seq, telling the sender that all
        // segments up to next_expected_seq - 1 were received
        .ack_num = htons(state->next_expected_seq),
        .recv_window = htons((uint16_t)free_window)
    };

    sendto(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

// moves segments from out_of_order to ready as long as the next expected seq is available
void process_ready_buffer(recv_buffer_state *state)
{
    while (true) {
        auto current = state->out_of_order.find(state->next_expected_seq);
        if (current == state->out_of_order.end()) {
            break;
        }

        state->ready.insert(state->ready.end(), current->second.begin(), current->second.end());
        state->out_of_order.erase(current);
        state->next_expected_seq++;
    }
}

// processes a received data segment
void process_data_segment(struct connection *con, const char *segment, int seg_len)
{
    if (seg_len < (int)sizeof(poli_tcp_data_hdr)) {
        return;
    }
    poli_tcp_data_hdr hdr;
    memcpy(&hdr, segment, sizeof(hdr));
    if (hdr.protocol_id != POLI_PROTOCOL_ID || hdr.type != TYPE_DATA) {
        return;
    }

    uint16_t seq = ntohs(hdr.seq_num);
    uint16_t data_len = ntohs(hdr.len);
    if (data_len > MAX_DATA_SIZE || seg_len < (int)(sizeof(poli_tcp_data_hdr) + data_len)) {
        return;
    }

    recv_buffer_state *state = (recv_buffer_state *)con->recv_state;
    const char *payload = segment + sizeof(poli_tcp_data_hdr);

    // if the received seq is exactly the next expected one
    if (seq == state->next_expected_seq) {
        // append it to ready
        state->ready.insert(state->ready.end(), payload, payload + data_len);
        state->next_expected_seq++;
        // promote the segments from out_of_order that are now in order because of this segment
        process_ready_buffer(state);
    } else if ((int16_t)(seq - state->next_expected_seq) > 0) {
        // seq greater than next_expected_seq: store it in out_of_order if it fits in the buffer and is not a duplicate
        if (buffered_bytes(state) + data_len <= state->max_buffer_size && state->out_of_order.find(seq) == state->out_of_order.end()) {
            state->out_of_order[seq] = vector<char>(payload, payload + data_len);
        }
    }
    // send an ack for duplicates and out of order segments as well
    send_ack(con);
}

int recv_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end()) {
        return -1;
    }
    int size = 0;

    while (1) {
        pthread_mutex_lock(&cons[conn_id]->con_lock);
        recv_buffer_state *recv_state = cons[conn_id]->recv_state;
        if (recv_state != NULL && !recv_state->ready.empty()) {
            // copy the available data into the application buffer
            size = min(len, (int)recv_state->ready.size());
            memcpy(buffer, recv_state->ready.data(), size);
            recv_state->ready.erase(recv_state->ready.begin(), recv_state->ready.begin() + size);
            // send an ack advertising the new free space in the buffer
            send_ack(cons[conn_id]);
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            return size;
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        // if no data is available, wait a little and check again
        usleep(1000);
    }
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {
        if (cons.size() == 0) {
            usleep(1000);
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        if (conn_id == -1 || cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        // if data was received, process the segment
        if (res > 0) {
            process_data_segment(cons[conn_id], segment, res);
        } else if (res == -1) {
            // on timeout, resend the current ack
            send_ack(cons[conn_id]);
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    return NULL;
}

int wait4connect(uint32_t ip, uint16_t port)
{
    if (listen_sock < 0) {
        return -1;
    }

    while (1) {
        struct sockaddr_in client_address;
        socklen_t socket_len = sizeof(client_address);
        char buf[MAX_SEGMENT_SIZE];
        // wait for a SYN on the listen socket
        ssize_t n = recvfrom(listen_sock, buf, sizeof(buf), 0, (struct sockaddr *)&client_address, &socket_len);
        // check if there was an error or a timeout
        if (n < 0) {
            continue; 
        }
        
        if ((size_t)n < sizeof(poli_tcp_ctrl_hdr)) {
            continue;
        }

        poli_tcp_ctrl_hdr hdr;
        memcpy(&hdr, buf, sizeof(hdr));
        if (hdr.protocol_id != POLI_PROTOCOL_ID || hdr.type != TYPE_SYN) {
            continue;
        }
        // a valid SYN was received, so try to create a new connection for it
        int data_socket = socket(AF_INET, SOCK_DGRAM, 0);
        if (data_socket < 0) {
            return -1;
        }

        // bind the new connection to a random free port
        struct sockaddr_in data_address = {
            .sin_family = AF_INET,
            .sin_port = 0,
            .sin_addr = { .s_addr = INADDR_ANY }
        };
        if (bind(data_socket, (struct sockaddr *)&data_address, sizeof(data_address)) < 0) {
            close(data_socket);
            return -1;
        }

        // find out the bound port so it can be sent back to the client in the SYN-ACK
        socklen_t address_len = sizeof(data_address);
        getsockname(data_socket, (struct sockaddr *)&data_address, &address_len);
        uint16_t data_port = ntohs(data_address.sin_port);

        int conn_id = next_conn_id++;
        assert(conn_id < MAX_CONNECTIONS);
        // initialize the connection structure
        struct connection *con = new connection{
            .sockfd = data_socket,
            .conn_id = (uint8_t)conn_id,
            .servaddr = client_address,
            .recv_state = new recv_buffer_state{
                .next_expected_seq = 1,
                .max_buffer_size = configured_recv_buffer
            }
        };

        // add the socket and the timer to the arrays used by poll
        data_fds[fdmax] = {
            .fd = con->sockfd,
            .events = POLLIN
        };
        timer_fds[fdmax] = {
            .fd = timerfd_create(CLOCK_REALTIME, 0),
            .events = POLLIN
        };
        struct itimerspec spec = {
            .it_interval = { .tv_sec = 1, .tv_nsec = 0 },
            .it_value    = { .tv_sec = 1, .tv_nsec = 0 }
        };

        cons.insert({conn_id, con});
        timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
        fdmax++;

        // send the SYN-ACK several times to reduce the chance of losing it
        poli_tcp_ctrl_hdr synack = {
            .protocol_id = POLI_PROTOCOL_ID,
            .conn_id = (uint8_t)conn_id,
            .type = TYPE_SYN_ACK,
            .recv_window = htons(data_port)
        };
        for (int i = 0; i < 5; i++) {
            sendto(listen_sock, &synack, sizeof(synack), 0, (struct sockaddr *)&client_address, socket_len);
        }

        return conn_id;
    }
}

void init_receiver(int recv_buffer_bytes)
{
    configured_recv_buffer = recv_buffer_bytes;

    // create the listen socket and bind it to port 8032
    listen_sock = socket(AF_INET, SOCK_DGRAM, 0);
    assert(listen_sock >= 0);

    struct sockaddr_in serv = {
        .sin_family = AF_INET,
        .sin_port = htons(8032),
        .sin_addr = { .s_addr = INADDR_ANY }
    };
    assert(bind(listen_sock, (struct sockaddr *)&serv, sizeof(serv)) >= 0);

    // start the thread for the receiver handler
    pthread_t thread1;
    int ret;

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
    pthread_detach(thread1);
}
