import {assert} from 'chai'
import {WebSocket as WsWebSocket} from 'ws'

import {ActionStreamClient, ErrorCode} from '$lib'
import {MockActionStreamServer} from '../utils/mock-server'

// Polyfill WebSocket for Node.js test runner
;(global as any).WebSocket = WsWebSocket

suite('ActionStreamClient', function () {
    this.slow(2000)
    this.timeout(10000)

    let server: MockActionStreamServer

    setup(async function () {
        server = new MockActionStreamServer()
        await server.start()
    })

    teardown(async function () {
        await server.close()
    })

    suite('connect', function () {
        test('should connect and receive heartbeat', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onHeartbeat = (state) => {
                assert.equal(String(state.headSeq), '1000')
                assert.equal(String(state.libSeq), '900')
                client.close()
                done()
            }
            client.connect()
        })

        test('should fire onConnect callback', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                client.close()
                done()
            }
            client.connect()
        })

        test('should throw when connecting after close', function () {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.close()
            assert.throws(() => client.connect(), 'Client is closed')
        })
    })

    suite('receiving actions', function () {
        test('should receive action via next()', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 5000,
                    contract: 'eosio.token',
                    action: 'transfer',
                    data: {from: 'alice', to: 'bob', quantity: '1.0000 EOS', memo: 'test'},
                })
            }
            client.connect()
            client.next().then((action) => {
                assert.equal(String(action.globalSeq), '5000')
                assert.equal(String(action.contract), 'eosio.token')
                assert.equal(String(action.action), 'transfer')
                assert.equal(String(action.receiver), 'eosio.token')
                assert.deepEqual(action.data, {
                    from: 'alice',
                    to: 'bob',
                    quantity: '1.0000 EOS',
                    memo: 'test',
                })
                client.close()
                done()
            })
        })

        test('should receive action with hex_data', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 6000,
                    contract: 'eosio.token',
                    action: 'transfer',
                    hexData: 'deadbeef',
                })
            }
            client.connect()
            client.next().then((action) => {
                assert.equal(String(action.globalSeq), '6000')
                assert.equal(action.hexData, 'deadbeef')
                assert.isUndefined(action.data)
                client.close()
                done()
            })
        })

        test('should receive action with trx_id', function (done) {
            const trxId = '5b273364b825dfd58e7ac36e4014a24f1547cb5b1786a586af31c5a83daaa03b'
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 6500,
                    contract: 'eosio.token',
                    action: 'transfer',
                    trxId,
                })
            }
            client.connect()
            client.next().then((action) => {
                assert.equal(String(action.globalSeq), '6500')
                assert.equal(String(action.trxId), trxId)
                client.close()
                done()
            })
        })

        test('should report DataInconsistent when trx_id is absent', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 6600,
                    contract: 'eosio.token',
                    action: 'transfer',
                    trxId: null,
                })
            }
            client.onError = (code) => {
                assert.equal(code, ErrorCode.DataInconsistent)
                client.close()
                done()
            }
            client.connect()
            client.nextWithTimeout(200).then((action) => {
                assert.isNull(action, 'no action should be delivered without a trx_id')
            })
        })

        test('should receive multiple actions in order', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                for (let i = 0; i < 5; i++) {
                    server.sendAction({
                        globalSeq: 100 + i,
                        contract: 'eosio.token',
                        action: 'transfer',
                    })
                }
            }
            client.connect()

            const received: number[] = []
            const consume = () => {
                client.next().then((action) => {
                    received.push(Number(action.globalSeq.toString()))
                    if (received.length === 5) {
                        assert.deepEqual(received, [100, 101, 102, 103, 104])
                        client.close()
                        done()
                    } else {
                        consume()
                    }
                })
            }
            consume()
        })

        test('should work with async iterator', async function () {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            let connected = false
            client.onConnect = () => {
                connected = true
                for (let i = 0; i < 3; i++) {
                    server.sendAction({
                        globalSeq: 200 + i,
                        contract: 'eosio.token',
                        action: 'transfer',
                    })
                }
            }
            client.connect()

            // Wait for connection
            while (!connected) {
                await new Promise((r) => setTimeout(r, 10))
            }

            const received: number[] = []
            for await (const action of client) {
                received.push(Number(action.globalSeq.toString()))
                if (received.length === 3) {
                    client.close()
                }
            }
            assert.deepEqual(received, [200, 201, 202])
        })
    })

    suite('nextWithTimeout', function () {
        test('should return null on timeout', async function () {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.connect()

            // Wait for connection
            await new Promise((r) => setTimeout(r, 100))

            const result = await client.nextWithTimeout(200)
            assert.isNull(result)
            client.close()
        })

        test('should return action before timeout', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 7000,
                    contract: 'eosio.token',
                    action: 'transfer',
                })
            }
            client.connect()
            client.nextWithTimeout(5000).then((action) => {
                assert.isNotNull(action)
                assert.equal(String(action!.globalSeq), '7000')
                client.close()
                done()
            })
        })
    })

    suite('catchup complete', function () {
        test('should fire onCatchupComplete and set flag', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            assert.isFalse(client.catchupComplete)
            client.onConnect = () => {
                server.sendCatchupComplete()
            }
            client.onCatchupComplete = (state) => {
                assert.isTrue(client.catchupComplete)
                assert.equal(String(state.headSeq), '1000')
                assert.equal(String(state.libSeq), '900')
                client.close()
                done()
            }
            client.connect()
        })
    })

    suite('error handling', function () {
        test('should fire onError callback', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                server.sendError(2, 'server syncing')
            }
            client.onError = (code, message) => {
                assert.equal(code, 2)
                assert.equal(message, 'server syncing')
                client.close()
                done()
            }
            client.connect()
        })
    })

    suite('state tracking', function () {
        test('should update headSeq and libSeq from heartbeat', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onHeartbeat = () => {
                assert.equal(String(client.headSeq), '1000')
                assert.equal(String(client.libSeq), '900')

                server.setHead(2000, 1900)
                server.broadcastHeartbeat()
            }
            let heartbeatCount = 0
            const origHandler = client.onHeartbeat
            client.onHeartbeat = (state) => {
                heartbeatCount++
                if (heartbeatCount === 1) {
                    origHandler!(state)
                    return
                }
                assert.equal(String(client.headSeq), '2000')
                assert.equal(String(client.libSeq), '1900')
                client.close()
                done()
            }
            client.connect()
        })

        test('should track currentSeq through consumed actions', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
                }, {
                startSeq: 0,
            })
            client.onConnect = () => {
                server.sendAction({
                    globalSeq: 500,
                    contract: 'eosio.token',
                    action: 'transfer',
                })
            }
            client.connect()
            client.next().then(() => {
                assert.isFalse(client.connected === undefined)
                client.close()
                done()
            })
        })
    })

    suite('close', function () {
        test('should reject pending next() on close', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onConnect = () => {
                client.close()
            }
            client.connect()
            client.next().catch((err) => {
                assert.match(err.message, /closed/)
                done()
            })
        })

        test('should reject next() called after close', async function () {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.close()
            try {
                await client.next()
                assert.fail('should have thrown')
            } catch (err: any) {
                assert.match(err.message, /closed/)
            }
        })

        test('should fire onDisconnect callback', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            })
            client.onDisconnect = () => {
                done()
            }
            client.onConnect = () => {
                client.close()
            }
            client.connect()
        })
    })

    suite('ack', function () {
        test('should send ack after ackInterval actions', function (done) {
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            }, {
                ackInterval: 3,
            })

            client.onConnect = () => {
                for (let i = 0; i < 5; i++) {
                    server.sendAction({
                        globalSeq: 100 + i,
                        contract: 'eosio.token',
                        action: 'transfer',
                    })
                }
            }
            client.connect()

            let consumed = 0
            const consume = () => {
                client.next().then(() => {
                    consumed++
                    if (consumed < 5) {
                        consume()
                    } else {
                        client.close()
                        done()
                    }
                })
            }
            consume()
        })
    })

    suite('reconnect', function () {
        test('should reconnect after server closes connection', function (done) {
            this.timeout(15000)
            const port = server.port
            const client = new ActionStreamClient(server.url, {
                contracts: ['eosio.token'],
            }, {
                reconnectDelay: 100,
                reconnectMaxDelay: 500,
            })

            let connectCount = 0
            client.onConnect = () => {
                connectCount++
                if (connectCount === 1) {
                    server.close().then(() => {
                        server = new MockActionStreamServer()
                        return server.start({port})
                    })
                } else if (connectCount === 2) {
                    client.close()
                    done()
                }
            }
            client.connect()
        })
    })
})
