/* Native synthetic context/map tests. No BPF syscall or network operation. */
#define SINOMO_COUNTER_OFFLINE
#include "ip-counter.bpf.c"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    struct __sk_buff packet = { .len = 1028, .mark = 72, .protocol = 0x0008 };
    struct __sk_buff before = packet;
    assert(sinomo_ip_ingress(&packet) == 1);
    assert(offline_counts[0].skb_bytes == 1028 && offline_counts[0].skb_packets == 1);
    assert(offline_counts[1].skb_bytes == 0 && offline_counts[1].skb_packets == 0);
    assert(memcmp(&packet, &before, sizeof(packet)) == 0);
    assert(sinomo_ip_egress(&packet) == 1);
    assert(offline_counts[1].skb_bytes == 1028 && offline_counts[1].skb_packets == 1);
    packet.len = 0;
    assert(sinomo_ip_ingress(&packet) == 1);
    assert(offline_counts[0].skb_packets == 2 && offline_counts[0].skb_bytes == 1028);
    packet.len = 60;
    offline_counts[0].skb_bytes = ~(__u64)0 - 59;
    assert(sinomo_ip_ingress(&packet) == 1 && offline_counts[0].overflow == 1);
    offline_counts[1].skb_packets = ~(__u64)0;
    assert(sinomo_ip_egress(&packet) == 1 && offline_counts[1].overflow == 1);
    before = packet;
    offline_lookup_missing = 1;
    struct counter saved[2];
    memcpy(saved, offline_counts, sizeof(saved));
    assert(sinomo_ip_ingress(&packet) == 1 && sinomo_ip_egress(&packet) == 1);
    assert(memcmp(saved, offline_counts, sizeof(saved)) == 0);
    assert(memcmp(&packet, &before, sizeof(packet)) == 0);
    puts("{\"checks\":13,\"verdict\":\"PASS\",\"kernel_bpf_calls\":0,\"acceptance\":false}");
    return 0;
}
