#include <iostream>
#include <vector>
#include <iomanip>
#include <string>
#include <set>
#include <algorithm>

using namespace std;

// ==============================================================================
// 1. CONFIGURATION & TYPES
// ==============================================================================
#define MAX_SEQ 7
#define WINDOW_SIZE 4 
#define TIMEOUT_TICKS_GBN 6  
#define TIMEOUT_TICKS_SR 10  
#define PROPAGATION_DELAY 2

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

// Math helper for circular windows
bool between(seq_nr a, seq_nr b, seq_nr c) {
    return (((a <= b) && (b < c)) || ((c < a) && (a <= b)) || ((b < c) && (c < a)));
}

void inc(seq_nr &k) {
    k = (k + 1) % (MAX_SEQ + 1);
}

// ==============================================================================
// 2. NETWORK LOGGER
// ==============================================================================
class NetworkLogger {
public:
    static void print_header(ProtocolMode mode) {
        string mode_str = (mode == BUFFERED_PROTOCOL_5) ? "WITH BUFFER & NAKs (Selective Repeat)" : "NO BUFFER (Go-Back-N)";
        int timeout = (mode == GO_BACK_N) ? TIMEOUT_TICKS_GBN : TIMEOUT_TICKS_SR;
        
        cout << "\n=== STARTING SIMULATION: " << mode_str << " ===\n";
        cout << "Timeout: " << timeout << " ticks\n";
        cout << left << setw(11) << "Time" 
             << left << setw(18) << "Sender Window" 
             << left << setw(10) << "Delivered" 
             << right << setw(20) << "Sender" << "   "
             << setw(12) << "Channel" << " "
             << left << "Receiver" << endl;
        cout << string(115, '-') << endl;
    }

    static void print_event(int tick, string window, int delivered, string sender_act, string arrow, string receiver_act) {
        cout << left << "Tick: " << setw(3) << tick << "| " 
             << left << setw(18) << window 
             << left << setw(10) << ("D:" + to_string(delivered))
             << right << setw(20) << sender_act << " "
             << setw(12) << arrow << " "
             << left << receiver_act << endl;
    }
    
    static void print_footer(int tick) {
        cout << "\n[!] SIMULATION ENDED: Reached tick (" << tick << ")." << endl;
        cout << string(115, '-') << "\n\n";
    }
};

// ==============================================================================
// 3. CHANNEL (The Physical Wire)
// ==============================================================================
class Channel {
private:
    vector<ChannelEvent> in_flight_events;
    set<int> packets_to_drop;

public:
    Channel(set<int> drops) : packets_to_drop(drops) {}

    bool should_drop(int packet_id) {
        if (packets_to_drop.count(packet_id)) {
            packets_to_drop.erase(packet_id);
            return true;
        }
        return false;
    }

    void inject(int delivery_tick, frame f, bool to_receiver) {
        in_flight_events.push_back({delivery_tick, f, to_receiver});
    }

    vector<ChannelEvent> get_arrivals(int current_tick) {
        vector<ChannelEvent> arrivals;
        vector<ChannelEvent> remaining;
        for (auto& ev : in_flight_events) {
            if (ev.delivery_tick == current_tick) arrivals.push_back(ev);
            else remaining.push_back(ev);
        }
        in_flight_events = remaining;
        return arrivals;
    }
};

// ==============================================================================
// 4. RECEIVER
// ==============================================================================
class Receiver {
private:
    ProtocolMode mode;
    seq_nr frame_expected;
    vector<bool> receiver_buffer; 
    vector<bool> nak_sent; 
    int packets_delivered;

public:
    Receiver(ProtocolMode m) : mode(m), frame_expected(0), packets_delivered(0) {
        receiver_buffer.resize(MAX_SEQ + 1, false);
        nak_sent.resize(MAX_SEQ + 1, false);
    }

    int get_delivered_count() const { return packets_delivered; }

    void process_arrival(frame f, Channel& channel, int tick, string& r_act) {
        if (f.seq == frame_expected) {
            r_act = "rcv pkt" + to_string(f.seq) + ", deliver pkt" + to_string(f.seq) + " ";
            if (mode == BUFFERED_PROTOCOL_5) nak_sent[frame_expected] = false; 
            inc(frame_expected);
            packets_delivered++;
            
            if (mode == BUFFERED_PROTOCOL_5) {
                while (receiver_buffer[frame_expected]) {
                    receiver_buffer[frame_expected] = false;
                    nak_sent[frame_expected] = false; 
                    r_act += "& pkt" + to_string(frame_expected) + " ";
                    inc(frame_expected);
                    packets_delivered++;
                }
            }

            seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1); 
            r_act += ", send ack" + to_string(ack_to_send);
            channel.inject(tick + PROPAGATION_DELAY, {ack_frame, 0, ack_to_send, ""}, false);
            
        } else {
            if (mode == BUFFERED_PROTOCOL_5 && between(frame_expected, f.seq, (frame_expected + WINDOW_SIZE) % (MAX_SEQ + 1))) {
                receiver_buffer[f.seq] = true; 
                r_act = "rcv pkt" + to_string(f.seq) + ", BUFFER";
                
                if (!nak_sent[frame_expected]) {
                    nak_sent[frame_expected] = true; 
                    r_act += ", send NAK" + to_string(frame_expected);
                    channel.inject(tick + PROPAGATION_DELAY, {nak_frame, 0, frame_expected, ""}, false);
                } else {
                    seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1);
                    r_act += ", (re)send ack" + to_string(ack_to_send);
                    channel.inject(tick + PROPAGATION_DELAY, {ack_frame, 0, ack_to_send, ""}, false);
                }
            } else {
                seq_nr ack_to_send = (frame_expected + MAX_SEQ) % (MAX_SEQ + 1);
                r_act = "rcv pkt" + to_string(f.seq) + ", DISCARD, (re)send ack" + to_string(ack_to_send);
                channel.inject(tick + PROPAGATION_DELAY, {ack_frame, 0, ack_to_send, ""}, false);
            }
        }
    }
};

// ==============================================================================
// 5. SENDER
// ==============================================================================
class Sender {
private:
    ProtocolMode mode;
    seq_nr next_frame_to_send;
    seq_nr ack_expected;
    int nbuffered;
    vector<int> timers;
    vector<string> out_buffer;
    vector<frame> gbn_retransmit_queue;
    int timeout_limit;

public:
    Sender(ProtocolMode m) : mode(m), next_frame_to_send(0), ack_expected(0), nbuffered(0) {
        timeout_limit = (mode == GO_BACK_N) ? TIMEOUT_TICKS_GBN : TIMEOUT_TICKS_SR;
        timers.resize(MAX_SEQ + 1, -1);
        out_buffer.resize((MAX_SEQ * 2) + MAX_SEQ, "");
    }

    string get_window_str() {
        string w = "";
        for (int i = 0; i <= MAX_SEQ; i++) {
            if (i == ack_expected) w += "[";
            w += to_string(i);
            if (i == (ack_expected + WINDOW_SIZE - 1) % (MAX_SEQ + 1)) w += "]"; 
            w += " ";
        }
        return w;
    }

    void tick_timers() {
        for (int i = 0; i <= MAX_SEQ; i++) {
            if (timers[i] > 0) timers[i]--;
        }
    }

    void process_ack(frame f, string& s_act) {
        seq_nr seq_next_frame = next_frame_to_send % (MAX_SEQ + 1);
        if (between(ack_expected, f.ack, seq_next_frame)) {
            while (between(ack_expected, f.ack, seq_next_frame)) {
                nbuffered--;
                timers[ack_expected] = -1; 
                gbn_retransmit_queue.erase(
                    remove_if(gbn_retransmit_queue.begin(), gbn_retransmit_queue.end(),
                        [this](const frame& qf) { return qf.seq == ack_expected; }),
                    gbn_retransmit_queue.end());
                inc(ack_expected);
            }
            s_act = "rcv ack" + to_string(f.ack);
        } else {
            s_act = "ignore dup ack" + to_string(f.ack);
        }
    }

    bool process_nak(frame f, Channel& channel, int tick, bool& locked, string& s_act) {
        seq_nr seq_next_frame = next_frame_to_send % (MAX_SEQ + 1);
        if (between(ack_expected, f.ack, seq_next_frame)) {
            if (!locked) {
                channel.inject(tick + PROPAGATION_DELAY, {data_frame, f.ack, 0, out_buffer[f.ack]}, true);
                s_act = "FAST re-send pkt" + to_string(f.ack);
                timers[f.ack] = timeout_limit; 
                locked = true;
                return true; // NAK processed
            }
            return false; // Interface busy, defer NAK
        }
        return true; // Old NAK, ignore
    }

    void check_timeouts(Channel& channel, int tick, bool& locked, int delivered) {
        seq_nr check_seq = ack_expected;
        for (int i = 0; i < nbuffered; i++) {
            if (timers[check_seq] == 0) {
                NetworkLogger::print_event(tick, get_window_str(), delivered, "TIMEOUT pkt" + to_string(check_seq), "", "");
                
                if (mode == GO_BACK_N) {
                    seq_nr temp = ack_expected;
                    for (int j = 0; j < nbuffered; j++) {
                        frame s = {data_frame, temp, 0, out_buffer[temp]};
                        auto it = find_if(gbn_retransmit_queue.begin(), gbn_retransmit_queue.end(),
                                        [&](const frame& qf) { return qf.seq == s.seq; });
                        if(it == gbn_retransmit_queue.end()) gbn_retransmit_queue.push_back(s); 
                        timers[temp] = -1; 
                        inc(temp);
                    }
                    break; 
                } else {
                    if (!locked) {
                        channel.inject(tick + PROPAGATION_DELAY, {data_frame, check_seq, 0, out_buffer[check_seq]}, true);
                        NetworkLogger::print_event(tick, get_window_str(), delivered, "re-send pkt" + to_string(check_seq), "---------->", "");
                        timers[check_seq] = timeout_limit; 
                        locked = true;
                    } 
                }
            }
            inc(check_seq);
        }
    }

    void drain_queue(Channel& channel, int tick, bool& locked, int delivered) {
        while (!gbn_retransmit_queue.empty()) {
            frame s = gbn_retransmit_queue.front();
            seq_nr seq_next_frame = next_frame_to_send % (MAX_SEQ + 1);
            
            if (between(ack_expected, s.seq, seq_next_frame)) {
                if (!locked) {
                    gbn_retransmit_queue.erase(gbn_retransmit_queue.begin());
                    channel.inject(tick + PROPAGATION_DELAY, s, true);
                    NetworkLogger::print_event(tick, get_window_str(), delivered, "re-send pkt" + to_string(s.seq), "---------->", "");
                    timers[s.seq] = timeout_limit; 
                    locked = true;
                    break; 
                } else {
                    break; 
                }
            } else {
                gbn_retransmit_queue.erase(gbn_retransmit_queue.begin());
            }
        }
    }

    void send_new(Channel& channel, int tick, bool& locked, int max_packets, int delivered) {
        if (!locked && gbn_retransmit_queue.empty() && nbuffered < WINDOW_SIZE && next_frame_to_send < max_packets) {
            seq_nr seq_to_send = next_frame_to_send % (MAX_SEQ + 1);
            out_buffer[seq_to_send] = "pkt" + to_string(seq_to_send);
            frame s = {data_frame, seq_to_send, 0, out_buffer[seq_to_send]};
            
            if (channel.should_drop(next_frame_to_send)) {
                NetworkLogger::print_event(tick, get_window_str(), delivered, "send pkt" + to_string(seq_to_send), "--X (LOSS)", "");
            } else {
                channel.inject(tick + PROPAGATION_DELAY, s, true);
                NetworkLogger::print_event(tick, get_window_str(), delivered, "send pkt" + to_string(seq_to_send), "---------->", "");
            }
            
            timers[seq_to_send] = timeout_limit; 
            next_frame_to_send++;
            nbuffered++;
            locked = true; 
        }
    }
};

// ==============================================================================
// 6. SIMULATOR (The Orchestrator)
// ==============================================================================
class Simulator {
private:
    ProtocolMode mode;
    int tick;
    int max_ticks; 
    int max_packets_to_send; 
    Sender sender;
    Receiver receiver;
    Channel channel;

public:
    Simulator(ProtocolMode m) 
        : mode(m), tick(0), max_ticks(150), max_packets_to_send(MAX_SEQ * 2), 
          sender(m), receiver(m), channel({2}) {} // Channel instantiated with drop at packet 2

    void run() {
        NetworkLogger::print_header(mode);

        while (receiver.get_delivered_count() < max_packets_to_send && tick < max_ticks) {
            bool tx_lock = false; // Transmission interface lock (1 packet/tick)

            // Step 1: Advance Timers
            sender.tick_timers();

            // Step 2: Deliver Packets from Wire
            vector<ChannelEvent> arrivals = channel.get_arrivals(tick);
            for (auto& ev : arrivals) {
                if (ev.to_receiver) {
                    string r_act = "";
                    receiver.process_arrival(ev.f, channel, tick, r_act);
                    NetworkLogger::print_event(tick, sender.get_window_str(), receiver.get_delivered_count(), "", "---------->", r_act);
                } else {
                    string s_act = "";
                    if (ev.f.kind == ack_frame) {
                        sender.process_ack(ev.f, s_act);
                        NetworkLogger::print_event(tick, sender.get_window_str(), receiver.get_delivered_count(), s_act, "<----------", "");
                    } else if (ev.f.kind == nak_frame) {
                        bool processed = sender.process_nak(ev.f, channel, tick, tx_lock, s_act);
                        if (processed) {
                            NetworkLogger::print_event(tick, sender.get_window_str(), receiver.get_delivered_count(), "rcv NAK" + to_string(ev.f.ack), "<----------", "");
                            NetworkLogger::print_event(tick, sender.get_window_str(), receiver.get_delivered_count(), s_act, "---------->", "");
                        } else {
                            channel.inject(tick + 1, ev.f, false); // Defer NAK to next tick
                        }
                    }
                }
            }

            // Step 3: Handle Timeouts
            sender.check_timeouts(channel, tick, tx_lock, receiver.get_delivered_count());

            // Step 4: Drain GBN queue
            sender.drain_queue(channel, tick, tx_lock, receiver.get_delivered_count());

            // Step 5: Send New Data
            sender.send_new(channel, tick, tx_lock, max_packets_to_send, receiver.get_delivered_count());

            tick++; 
        }
        
        NetworkLogger::print_footer(tick);
    }
};

// ==============================================================================
// 7. MAIN ENTRY
// ==============================================================================
int main() {
    Simulator gbn(GO_BACK_N);
    gbn.run();

    Simulator sr(BUFFERED_PROTOCOL_5);
    sr.run();

    return 0;
}