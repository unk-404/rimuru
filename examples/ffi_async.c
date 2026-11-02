/*
 * Minimal C11 example for the Rimuru C ABI 1.0.
 *
 * Build (Linux/macOS):
 *   cargo build --release --lib
 *   cc -std=c11 -Iinclude examples/ffi_async.c \
 *      target/release/librimuru.a -lpthread -ldl -lm -o /tmp/rimuru-ffi
 *
 * Windows (MSVC):
 *   cargo build --release --lib
 *   cl /std:c11 /Iinclude examples\ffi_async.c target\release\rimuru.lib \
 *      ws2_32.lib userenv.lib bcrypt.lib ntdll.lib
 *
 * The example shows the frozen async protocol, not a blocking convenience wrap:
 *   poll_batch -> empty? arm(epoch) -> READY? retry / ARMED? wait -> poll_batch
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "rimuru.h"

static RmEndpointConfig default_config(void) {
    RmEndpointConfig config;
    RmStatus status = rm_endpoint_config_init(RM_ABI_VERSION, sizeof(config), &config);
    if (status != RM_OK) {
        fprintf(stderr, "config_init failed: %d\n", (int)status);
    }
    return config;
}

static RmAddress loopback(uint16_t port) {
    RmAddress address;
    memset(&address, 0, sizeof(address));
    address.family = 4;
    address.port = port;
    address.address[0] = 127;
    address.address[3] = 1;
    return address;
}

/*
 * Wait for the next completion without ever sleeping on a stale epoch:
 * poll first, then arm the proven-empty epoch, then wait on the OS object.
 */
static RmStatus next_event(RmHandle endpoint, RmEvent *out) {
    RmHandle cq = 0;
    RmHandle waiter = 0;
    RmStatus status = rm_endpoint_default_cq(endpoint, &cq);
    if (status != RM_OK) return status;
    status = rm_waiter_create(cq, &waiter);
    if (status != RM_OK) return status;
    for (;;) {
        uint32_t count = 0;
        uint64_t epoch = 0;
        status = rm_endpoint_poll_batch(endpoint, out, sizeof(*out), 1, &count, &epoch);
        if (count == 1) {
            rm_handle_release(waiter);
            rm_handle_release(cq);
            return RM_OK;
        }
        if (status != RM_WOULD_BLOCK && status != RM_CLOSED) {
            rm_handle_release(waiter);
            rm_handle_release(cq);
            return status;
        }
        status = rm_waiter_arm(waiter, epoch);
        if (status == RM_READY) continue;
        if (status != RM_ARMED) {
            rm_handle_release(waiter);
            rm_handle_release(cq);
            return status;
        }
        status = rm_waiter_wait(waiter, 2000000000ull);
        if (status != RM_OK && status != RM_TIMED_OUT) {
            rm_handle_release(waiter);
            rm_handle_release(cq);
            return status;
        }
        rm_waiter_reset(waiter);
    }
}

int main(void) {
    RmEndpointConfig config = default_config();
    RmAddress any = loopback(0);
    RmHandle sender = 0;
    RmHandle receiver = 0;
    if (rm_endpoint_bind(&config, &any, &sender) != RM_OK ||
        rm_endpoint_bind(&config, &any, &receiver) != RM_OK) {
        fprintf(stderr, "bind failed\n");
        return 1;
    }

    RmAddress peer;
    if (rm_endpoint_local_address(receiver, sizeof(peer), &peer) != RM_OK) {
        fprintf(stderr, "local_address failed\n");
        return 1;
    }

    const char *payload = "hello from C";
    RmHandle ticket = 0;
    RmStatus status = rm_send_copy(sender, &peer, (const uint8_t *)payload,
                                   (uint32_t)strlen(payload), 0xC0FFEE, &ticket);
    if (status != RM_OK) {
        fprintf(stderr, "send_copy failed: %d\n", (int)status);
        return 1;
    }

    RmEvent event;
    memset(&event, 0, sizeof(event));
    if (next_event(sender, &event) != RM_OK || event.kind != RM_EVENT_SEND_TERMINAL) {
        fprintf(stderr, "no send terminal\n");
        return 1;
    }
    printf("send terminal: ticket=%llu status=%d\n",
           (unsigned long long)event.ticket_id, (int)event.status);
    rm_handle_release(ticket);

    for (;;) {
        memset(&event, 0, sizeof(event));
        if (next_event(receiver, &event) != RM_OK) {
            fprintf(stderr, "no message\n");
            return 1;
        }
        if (event.kind == RM_EVENT_MESSAGE) {
            const uint8_t *data = NULL;
            uint32_t len = 0;
            RmHandle view = 0;
            if (rm_message_view(event.message, &data, &len, &view) != RM_OK) {
                fprintf(stderr, "message_view failed\n");
                return 1;
            }
            printf("received %u bytes: %.*s\n", len, (int)len, (const char *)data);
            rm_handle_release(view);
            rm_handle_release(event.message);
            break;
        }
        if (event.message != 0) rm_handle_release(event.message);
    }

    rm_endpoint_close(receiver);
    rm_endpoint_close(sender);
    rm_handle_release(sender);
    rm_handle_release(receiver);
    return 0;
}
