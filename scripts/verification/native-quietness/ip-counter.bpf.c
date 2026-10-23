/* Review artifact only: no loader or attachment is supplied by this file.
 * One fresh map/object per disjoint, manifest-bound cgroup root.
 * Counts skb length at the cgroup hooks, NOT interface-equivalent bytes.
 */
#include <linux/bpf.h>

struct counter {
    __u64 skb_bytes;
    __u64 skb_packets;
    __u64 overflow;
};

#ifdef SINOMO_COUNTER_OFFLINE
#include <stddef.h>
#define SEC(name)
static struct counter offline_counts[2];
static int offline_lookup_missing;
static int counts;
static void *bpf_map_lookup_elem(const void *map, const void *key) {
    (void)map;
    __u32 index = *(const __u32 *)key;
    return offline_lookup_missing || index >= 2 ? NULL : &offline_counts[index];
}
#else
#include <bpf/bpf_helpers.h>
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 2);
    __uint(map_flags, BPF_F_RDONLY);
    __type(key, __u32);
    __type(value, struct counter);
} counts SEC(".maps");
#endif

static __attribute__((always_inline)) inline int count_skb(struct __sk_buff *skb, __u32 direction) {
    struct counter *value = bpf_map_lookup_elem(&counts, &direction);
    if (value) {
        __u64 length = skb->len;
        __u64 old_bytes = __sync_fetch_and_add(&value->skb_bytes, length);
        __u64 old_packets = __sync_fetch_and_add(&value->skb_packets, 1);
        if (old_bytes > ~(__u64)0 - length || old_packets == ~(__u64)0)
            __sync_fetch_and_or(&value->overflow, 1);
    }
    /* Never drop, mark, redirect or rewrite a packet, even on counter failure. */
    return 1;
}

SEC("cgroup_skb/ingress")
int sinomo_ip_ingress(struct __sk_buff *skb) {
    return count_skb(skb, 0);
}

SEC("cgroup_skb/egress")
int sinomo_ip_egress(struct __sk_buff *skb) {
    return count_skb(skb, 1);
}

#ifndef SINOMO_COUNTER_OFFLINE
char LICENSE[] SEC("license") = "GPL";
#endif
