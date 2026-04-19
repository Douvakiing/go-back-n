#include <iostream>
#include <vector>
#include <iomanip>
#include <string>
#include <set>
#include <algorithm>

using namespace std;

#define MAX_SEQ 7
#define TIMEOUT_TICKS 10 
#define WINDOW_SIZE 6

typedef unsigned int seq_nr;
typedef enum { data_frame, ack_frame, nak_frame } frame_kind;
enum ProtocolMode { GO_BACK_N, BUFFERED_PROTOCOL_5 };

struct frame {
    frame_kind kind;
    seq_nr seq;
    seq_nr ack;
    string info;
};

struct ChannelEvent {
    int delivery_tick;
    frame f;
    bool to_receiver; 
};

bool between(seq_nr a, seq_nr b, seq_nr c) {
    if (((a <= b) && (b < c)) || ((c < a) && (a <= b)) || ((b < c) && (c < a)))
        return true;
    return false;
}

void inc(seq_nr &k) {
    k = (k + 1) % (MAX_SEQ + 1);
}

class NetworkSimulator {
private:
    ProtocolMode mode;
    int tick;
    int max_ticks; 
    int max_packets_to_send; 
    int packets_delivered;

    // Sender State
    seq_nr next_frame_to_send;
    seq_nr ack_expected;
    int nbuffered;
    vector<int> timers;
    vector<string> out_buffer;

    // Receiver State
    seq_nr frame_expected;
    vector<bool> receiver_buffer; 
    vector<bool> nak_sent; 

    // Channel
    vector<ChannelEvent> channel;
    set<int> packets_to_drop; 

public:
    NetworkSimulator(ProtocolMode m) : mode(m) {
        tick = 0;
        max_ticks = 150;            
        max_packets_to_send = MAX_SEQ * 1.5;  // roughly to loop completely 
        packets_delivered = 0;

        next_frame_to_send = 0;
        ack_expected = 0;
        nbuffered = 0;
        timers.resize(MAX_SEQ + 1, -1);
        out_buffer.resize(max_packets_to_send + MAX_SEQ, "");

        frame_expected = 0;
        receiver_buffer.resize(MAX_SEQ + 1, false);
        nak_sent.resize(MAX_SEQ + 1, false);

        // Drop packets (errors)
        packets_to_drop = {2}; 
    }

    string get_sender_window_str() {
        string w = "";
        for (int i = 0; i <= MAX_SEQ; i++) {
            if (i == ack_expected) w += "[";
            w += to_string(i);
            if (i == (ack_expected + WINDOW_SIZE - 1) % (MAX_SEQ + 1)) w += "]"; // WINDOWSIZE
            w += " ";
        }
        return w;
    }

    void print_event(string sender_action, string arrow, string receiver_action) {
        cout << left << setw(20) << get_sender_window_str() 
             << right << setw(22) << sender_action << " "
             << setw(12) << arrow << " "
             << left << receiver_action << endl;
    }

    // NEW HELPER: We moved the send logic here so we can call it anytime!
    void send_new_packets(vector<ChannelEvent>& target_channel) {
        while (nbuffered < WINDOW_SIZE && next_frame_to_send < max_packets_to_send) {
            seq_nr seq_to_send = next_frame_to_send % (MAX_SEQ + 1);
            out_buffer[seq_to_send] = "pkt" + to_string(seq_to_send);
            frame s = {data_frame, seq_to_send, 0, out_buffer[seq_to_send]};
            
            if (packets_to_drop.count(next_frame_to_send)) {
                print_event("send pkt" + to_string(seq_to_send), "--X (LOSS)", "");
                packets_to_drop.erase(next_frame_to_send); 
            } else {
                target_channel.push_back({tick + 2, s, true}); 
                print_event("send pkt" + to_string(seq_to_send), "---------->", "");
            }
            
            timers[seq_to_send] = TIMEOUT_TICKS; 
            next_frame_to_send++;
            nbuffered++;
        }
    }

    void run() {
        string mode_str = (mode == BUFFERED_PROTOCOL_5) ? "WITH BUFFER & NAKs " : "NO BUFFER (Go-Back-N)";
        cout << "\n=== STARTING SIMULATION: " << mode_str << " ===\n";
        cout << left << setw(10) << "Sender Window (N="<<(WINDOW_SIZE)<<")" 
             << right << setw(MAX_SEQ*3) << "Sender" << " "
             << setw(25) << "Channel" << " "
             << left << "Receiver" << endl;
        cout << string(95, '-') << endl;

        while (packets_delivered < max_packets_to_send && tick < max_ticks) {
            bool action_taken = false;

            // 1. SENDER: Fill the initial window at the start of a tick
            send_new_packets(channel);

            // 2. TIMERS: Handle Timeouts
            for (int i = 0; i <= MAX_SEQ; i++) {
                if (timers[i] > 0) timers[i]--;
            }

            seq_nr check_seq = ack_expected;
            for (int i = 0; i < nbuffered; i++) {
                if (timers[check_seq] == 0) {
                    print_event("TIMEOUT pkt" + to_string(check_seq), "", "");
                    
                    if (mode == GO_BACK_N) {
                        seq_nr temp = ack_expected;
                        for (int j = 0; j < nbuffered; j++) {
                            frame s = {data_frame, temp, 0, out_buffer[temp]};
                            channel.push_back({tick + 2, s, true});
                            print_event("re-send pkt" + to_string(temp), "---------->", "");
                            timers[temp] = TIMEOUT_TICKS;
                            inc(temp);
                        }
                        action_taken = true;
                        break; 
                    } else {
                        frame s = {data_frame, check_seq, 0, out_buffer[check_seq]};
                        channel.push_back({tick + 2, s, true});
                        print_event("re-send pkt" + to_string(check_seq), "---------->", "");
                        timers[check_seq] = TIMEOUT_TICKS;
                        action_taken = true;
                    }
                }
                inc(check_seq);
            }

            // 3. CHANNEL: Process Arrivals
            vector<ChannelEvent> next_channel;
            for (auto& ev : channel) {
                if (ev.delivery_tick == tick) {
                    if (ev.to_receiver) {
                        // RECEIVER LOGIC
                        if (ev.f.seq == frame_expected) {
                            string r_act = "rcv pkt" + to_string(ev.f.seq) + ", deliver pkt" +  to_string(ev.f.seq) + " ";
                            
                            if (mode == BUFFERED_PROTOCOL_5) nak_sent[frame_expected] = false; 
                            inc(frame_expected);
                            packets_delivered++;
                            
                            if (mode == BUFFERED_PROTOCOL_5) {
                                while (receiver_buffer[frame_expected]) {
                                    receiver_buffer[frame_expected] = false;
                                    nak_sent[frame_expected] = false; 
                                    r_act += " & pkt" + to_string(frame_expected);
                                    inc(frame_expected);
                                    packets_delivered++;
                                }
                            }

                            seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1); 
                            r_act += ", send ack" + to_string(ack_to_send);
                            
                            frame a = {ack_frame, 0, ack_to_send, ""};
                            next_channel.push_back({tick + 2, a, false});
                            print_event("", "---------->", r_act);
                            
                        } else {
                            if (mode == BUFFERED_PROTOCOL_5 && between(frame_expected, ev.f.seq, (frame_expected + WINDOW_SIZE) % (MAX_SEQ + 1))) {
                                receiver_buffer[ev.f.seq] = true; 
                                string r_act = "rcv pkt" + to_string(ev.f.seq) + ", BUFFER";
                                
                                if (!nak_sent[frame_expected]) {
                                    nak_sent[frame_expected] = true; 
                                    r_act += ", send NAK" + to_string(frame_expected);
                                    frame n = {nak_frame, 0, frame_expected, ""};
                                    next_channel.push_back({tick + 2, n, false});
                                } else {
                                    seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1);
                                    r_act += ", (re)send ack" + to_string(ack_to_send);
                                    frame a = {ack_frame, 0, ack_to_send, ""};
                                    next_channel.push_back({tick + 2, a, false});
                                }
                                print_event("", "---------->", r_act);
                            } else {
                                seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1);
                                print_event("", "---------->", "rcv pkt" + to_string(ev.f.seq) + ", DISCARD, (re)send ack" + to_string(ack_to_send));
                                frame a = {ack_frame, 0, ack_to_send, ""};
                                next_channel.push_back({tick + 2, a, false});
                            }
                        }
                    } else {
                        // SENDER LOGIC
                        seq_nr seq_next_frame = next_frame_to_send % (MAX_SEQ + 1);
                        
                        if (ev.f.kind == ack_frame) {
                            if (between(ack_expected, ev.f.ack, seq_next_frame)) {
                                while (between(ack_expected, ev.f.ack, seq_next_frame)) {
                                    nbuffered--;
                                    timers[ack_expected] = -1; 
                                    inc(ack_expected);
                                }
                                print_event("rcv ack" + to_string(ev.f.ack), "<----------", "");
                                
                                // NEW: As soon as an ACK slides the window, trigger a send instantly!
                                send_new_packets(next_channel);
                                
                            } else {
                                print_event("ignore dup ack" + to_string(ev.f.ack), "<----------", "");
                            }
                        } else if (ev.f.kind == nak_frame && mode == BUFFERED_PROTOCOL_5) {
                            if (between(ack_expected, ev.f.ack, seq_next_frame)) {
                                print_event("rcv NAK" + to_string(ev.f.ack), "<----------", "");
                                
                                frame s = {data_frame, ev.f.ack, 0, out_buffer[ev.f.ack]};
                                next_channel.push_back({tick + 2, s, true});
                                print_event("FAST re-send pkt" + to_string(ev.f.ack), "---------->", "");
                                
                                timers[ev.f.ack] = TIMEOUT_TICKS; 
                            }
                        }
                    }
                    action_taken = true;
                } else {
                    next_channel.push_back(ev); 
                }
            }
            channel = next_channel;
            tick++;
        }
        
        if (tick >= max_ticks) {
            cout << "\n[!] SIMULATION ENDED: Reached max tick limit (" << max_ticks << ")." << endl;
        }
        cout << string(95, '-') << "\n\n";
    }
};

int main() {
    NetworkSimulator gbn(GO_BACK_N);
    gbn.run();

    NetworkSimulator sr(BUFFERED_PROTOCOL_5);
    sr.run();

    return 0;
}