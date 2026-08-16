# @wharfkit/actionstream

TypeScript client for subscribing to real-time action streams from a [Roborovski](https://github.com/greymass/roborovski) actionindex service.

## Installation

```bash
npm install @wharfkit/actionstream
# or
yarn add @wharfkit/actionstream
```

## Usage

### Async Iterator

```typescript
import {ActionStreamClient} from '@wharfkit/actionstream'

const client = new ActionStreamClient('wss://example.com/v1/actionstream', {
    contracts: ['eosio.token'],
})

client.connect()

for await (const action of client) {
    console.log(String(action.globalSeq), String(action.contract) + '::' + String(action.action))
    console.log(action.data)
}
```

### Pull-based

```typescript
const client = new ActionStreamClient('wss://example.com/v1/actionstream', {
    contracts: ['eosio.token'],
    receivers: ['myaccount'],
})

client.connect()

const action = await client.next()
const actionOrNull = await client.nextWithTimeout(5000)
```

### Options

```typescript
const client = new ActionStreamClient(url, filter, {
    startSeq: '48000000000',    // resume from a specific sequence number, or 'head'
    decode: true,               // request ABI-decoded action data (default: true)
    reconnectDelay: 1000,       // initial reconnect delay in ms (default: 1000)
    reconnectMaxDelay: 30000,   // max reconnect delay in ms (default: 30000)
    ackInterval: 1000,          // sequence span between ack messages (default: 1000)
    queueSize: 1000,            // buffered actions before overflow recovery (default: 1000)
    healthyThreshold: 10000,    // ms live before a connection counts as healthy (default: 10000)
})
```

Omitting `startSeq` replays all retained history from the start of the stream. Pass `'head'` to receive only the actions that arrive after connecting.

### Delivery Guarantee

Paired with an actionindex server that emits `sub_seq`, the stream delivers every action matching the filter with `global_seq >= start_seq` at least once and in order. Duplicates are possible across reconnects; consumers that need exactly-once effects deduplicate by `globalSeq`. A server-side delivery fault surfaces as an automatic reconnect that resumes from the last accepted action, so faults degrade to latency rather than data loss. The guarantee assumes the server retains full history, which actionindex does.

The server assigns each connection a `sub_seq` counter. A skip in that counter signals a protocol-level delivery fault; the client resumes automatically from the last accepted sequence and fires `onGap`.

```typescript
client.onGap = (gap) => {
    console.log('gap: expected', gap.expected, 'received', gap.received, 'resuming from', String(gap.resumeSeq))
}
```

Servers that predate `sub_seq` do not send it, and the client disables the check for that connection.

### Overflow

A consumer that drains slower than actions arrive fills the client's buffer. On a full buffer the client keeps everything it has already accepted, reconnects, and resumes from the sequence after the last accepted action, so no action is skipped. Every occurrence fires `onOverflow` and increments `overflowCount`.

```typescript
client.onOverflow = (info) => {
    console.log('overflow at', String(info.droppedFrom), 'resuming from', String(info.resumeSeq))
}
```

Acks are sent as actions are consumed, and the server's own flow control watches them: once its unacked window (10,000 actions) closes, it drops rather than blocks. Keep `queueSize` well below that window. Raising it past the window moves the loss server-side, where the client cannot observe it.

### Reconnect Backoff

Every dropped connection schedules a redial after the current backoff delay, which starts at `reconnectDelay` and doubles up to `reconnectMaxDelay`. The delay returns to `reconnectDelay` once a connection proves healthy, meaning it reached `catchup_complete` and then stayed live for `healthyThreshold` milliseconds. Connections that die sooner leave the backoff growing, so a server that repeatedly disconnects a client right after catch-up (error code 6, for example) sees the redial interval widen instead of a flat retry loop. Overflow and gap recovery reconnect under the same rule.

### Lifecycle Callbacks

```typescript
client.onConnect = () => {}
client.onDisconnect = () => {}
client.onHeartbeat = (state) => {
    console.log('head:', String(state.headSeq), 'lib:', String(state.libSeq))
}
client.onCatchupComplete = (state) => {}
client.onError = (code, message) => {}
client.onOverflow = (overflow) => {}
client.onGap = (gap) => {}
```

`onError` receives the server's error code and message. The `ErrorCode` enum names each code.

| Code | Name | Meaning |
|------|------|---------|
| 1 | `InvalidRequest` | The subscribe message was malformed or the filter was rejected |
| 2 | `ServerSyncing` | The server is still catching up to the chain |
| 3 | `MaxClients` | The server is at its connection limit |
| 4 | `NoActions` | The filter matches no retained actions |
| 5 | `DataInconsistent` | An action arrived without the fields the protocol requires |
| 6 | `ResyncRequired` | The subscription lagged too far behind and the server closed it |

Code 6 arrives with a disconnect. The client reconnects on its own and resumes from the last accepted sequence.

### State

```typescript
client.headSeq        // UInt64 - latest sequence on the server
client.libSeq         // UInt64 - last irreversible sequence
client.connected      // boolean
client.catchupComplete // boolean
client.overflowCount  // number - buffer overflows recovered since construction
```

### Filter

Subscriptions accept three optional filter dimensions. An action matches if it satisfies all specified dimensions. Omitted dimensions are unconstrained.

```typescript
{
    contracts: ['eosio.token'],        // contract account names
    receivers: ['myaccount'],          // notification receivers
    actions: ['transfer', 'issue'],    // action names
}
```

## Development

```
make          # build
make test     # run tests
make check    # lint
```

## License

BSD-3-Clause
