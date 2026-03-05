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
    startSeq: '48000000000',   // resume from a specific sequence number
    decode: true,               // request ABI-decoded action data (default: true)
    reconnectDelay: 1000,       // initial reconnect delay in ms (default: 1000)
    reconnectMaxDelay: 30000,   // max reconnect delay in ms (default: 30000)
    ackInterval: 1000,          // actions between ack messages (default: 1000)
})
```

### Lifecycle Callbacks

```typescript
client.onConnect = () => {}
client.onDisconnect = () => {}
client.onHeartbeat = (state) => {
    console.log('head:', String(state.headSeq), 'lib:', String(state.libSeq))
}
client.onCatchupComplete = (state) => {}
client.onError = (code, message) => {}
```

### State

```typescript
client.headSeq        // UInt64 - latest sequence on the server
client.libSeq         // UInt64 - last irreversible sequence
client.connected      // boolean
client.catchupComplete // boolean
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
