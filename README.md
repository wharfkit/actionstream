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
})
```

Omitting `startSeq` replays all retained history from the start of the stream. Pass `'head'` to
receive only the actions that arrive after connecting.

### Overflow

A consumer that drains slower than actions arrive fills the client's buffer. On a full buffer the
client keeps everything it has already accepted, reconnects, and resumes from the sequence after
the last accepted action, so no action is skipped. Every occurrence fires `onOverflow` and
increments `overflowCount`.

```typescript
client.onOverflow = (info) => {
    console.log('overflow at', String(info.droppedFrom), 'resuming from', String(info.resumeSeq))
}
```

Acks are sent as actions are consumed, and the server's own flow control watches them: once its
unacked window (10,000 actions) closes, it drops rather than blocks. Keep `queueSize` well below
that window. Raising it past the window moves the loss server-side, where the client cannot
observe it.

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
```

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
