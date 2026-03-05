import {ActionStreamClient} from '../src'

const host = process.env.ACTIONSTREAM_HOST
if (!host) {
    console.error('Set ACTIONSTREAM_HOST (e.g. actionstream.example.com)')
    process.exit(1)
}

const client = new ActionStreamClient(`wss://${host}/v1/actionstream`, {
    contracts: ['eosio.token'],
})

client.onConnect = () => {
    console.log('Connected')
}

client.onDisconnect = () => {
    console.log('Disconnected')
}

client.onHeartbeat = (state) => {
    console.log('Heartbeat:', 'head=' + state.headSeq, 'lib=' + state.libSeq)
}

client.onCatchupComplete = (state) => {
    console.log('Catchup complete:', 'head=' + state.headSeq, 'lib=' + state.libSeq)
}

client.onError = (code, message) => {
    console.log('Error:', code, message)
}

client.connect()

let count = 0
for await (const action of client) {
    console.log(
        '#' + String(action.globalSeq),
        String(action.contract) + '::' + String(action.action),
        '→',
        String(action.receiver),
        action.data ? JSON.stringify(action.data) : action.hexData
    )
    count++
    if (count >= 20) {
        console.log('Received 20 actions, closing.')
        client.close()
    }
}
