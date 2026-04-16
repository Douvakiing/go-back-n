#include <iostream>
#include <vector>
#include <iomanip>
#include <string>
#include <set>
#include <algorithm>

using namespace std;

#define MAX_SEQ 7
#define TIMEOUT_TICKS 10 

typedef unsigned int seq_nr;
typedef enum { data_frame, ack_frame, nak_frame } frame_kind;
enum ProtocolMode { GO_BACK_N, SELECTIVE_REPEAT };

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
        max_packets_to_send = 10;   
        packets_delivered = 0;

        next_frame_to_send = 0;
        ack_expected = 0;
        nbuffered = 0;
        timers.resize(MAX_SEQ + 1, -1);
        out_buffer.resize(max_packets_to_send + MAX_SEQ, "");

        frame_expected = 0;
        receiver_buffer.resize(MAX_SEQ + 1, false);
        nak_sent.resize(MAX_SEQ + 1, false);

        // Drop packets 2 and 5
        packets_to_drop = {2, 6}; 
    }

    string get_sender_window_str() {
        string w = "";
        for (int i = 0; i <= MAX_SEQ; i++) {
            if (i == ack_expected) w += "[";
            w += to_string(i);
            if (i == (ack_expected + 4 - 1) % (MAX_SEQ + 1)) w += "]"; 
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

    void run() {
        string mode_str = (mode == SELECTIVE_REPEAT) ? "WITH BUFFER & NAKs (Selective Repeat)" : "NO BUFFER (Go-Back-N)";
        cout << "\n=== STARTING SIMULATION: " << mode_str << " ===\n";
        cout << left << setw(20) << "Sender Window (N=4)" 
             << right << setw(22) << "Sender" << " "
             << setw(12) << "Channel" << " "
             << left << "Receiver" << endl;
        cout << string(95, '-') << endl;

        while (packets_delivered < max_packets_to_send && tick < max_ticks) {
            bool action_taken = false;

            // 1. SENDER: Inject new packets
            while (nbuffered < 4 && next_frame_to_send < max_packets_to_send) {
                seq_nr seq_to_send = next_frame_to_send % (MAX_SEQ + 1);
                out_buffer[seq_to_send] = "pkt" + to_string(seq_to_send);
                frame s = {data_frame, seq_to_send, 0, out_buffer[seq_to_send]};
                
                if (packets_to_drop.count(next_frame_to_send)) {
                    // FIX 1: Print wrapped seq_to_send instead of next_frame_to_send
                    print_event("send pkt" + to_string(seq_to_send), "--X (LOSS)", "");
                    packets_to_drop.erase(next_frame_to_send); 
                } else {
                    channel.push_back({tick + 2, s, true}); 
                    print_event("send pkt" + to_string(seq_to_send), "---------->", "");
                }
                
                timers[seq_to_send] = TIMEOUT_TICKS; 
                next_frame_to_send++;
                nbuffered++;
                action_taken = true;
            }

            // 2. TIMERS: Handle Timeouts
            // First, decrement all active timers
            for (int i = 0; i <= MAX_SEQ; i++) {
                if (timers[i] > 0) timers[i]--;
            }

            // FIX 2: Check for timeouts in window order (starting from ack_expected)
            seq_nr check_seq = ack_expected;
            for (int i = 0; i < nbuffered; i++) {
                if (timers[check_seq] == 0) {
                    print_event("TIMEOUT pkt" + to_string(check_seq), "", "");
                    
                    if (mode == GO_BACK_N) {
                        // Go-Back-N: Resend ALL unacknowledged packets
                        seq_nr temp = ack_expected;
                        for (int j = 0; j < nbuffered; j++) {
                            frame s = {data_frame, temp, 0, out_buffer[temp]};
                            channel.push_back({tick + 2, s, true});
                            print_event("re-send pkt" + to_string(temp), "---------->", "");
                            timers[temp] = TIMEOUT_TICKS;
                            inc(temp);
                        }
                        action_taken = true;
                        break; // All timers just got reset, break the loop
                    } else {
                        // Selective Repeat: Resend ONLY the timed-out packet
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
                            string r_act = "rcv pkt" + to_string(ev.f.seq) + ", deliver";
                            
                            if (mode == SELECTIVE_REPEAT) nak_sent[frame_expected] = false; 
                            inc(frame_expected);
                            packets_delivered++;
                            
                            if (mode == SELECTIVE_REPEAT) {
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
                            if (mode == SELECTIVE_REPEAT && between(frame_expected, ev.f.seq, (frame_expected + 4) % (MAX_SEQ + 1))) {
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
                            } else {
                                print_event("ignore dup ack" + to_string(ev.f.ack), "<----------", "");
                            }
                        } else if (ev.f.kind == nak_frame && mode == SELECTIVE_REPEAT) {
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

    NetworkSimulator sr(SELECTIVE_REPEAT);
    sr.run();

    return 0;
}