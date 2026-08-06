import {WebSocketServer, WebSocket as WsWebSocket} from 'ws'
import {AddressInfo} from 'net'

export interface MockServerOptions {
    heartbeatInterval?: number
}

export class MockActionStreamServer {
    private wss: WebSocketServer | null = null
    private clients: Set<WsWebSocket> = new Set()
    private _port = 0
    private _headSeq = 1000
    private _libSeq = 900
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null

    get port(): number {
        return this._port
    }

    get url(): string {
        return `ws://localhost:${this._port}`
    }

    get clientCount(): number {
        return this.clients.size
    }

    start(options?: MockServerOptions & {port?: number}): Promise<void> {
        return new Promise((resolve) => {
            const listenPort = (options && options.port) || 0
            this.wss = new WebSocketServer({port: listenPort}, () => {
                this._port = (this.wss!.address() as AddressInfo).port
                resolve()
            })

            this.wss.on('connection', (ws) => {
                this.clients.add(ws)
                ws.on('close', () => this.clients.delete(ws))
                ws.once('message', (data) => this.handleSubscribe(ws, data))
            })

            if (options && options.heartbeatInterval) {
                this.heartbeatTimer = setInterval(() => {
                    this.broadcastHeartbeat()
                }, options.heartbeatInterval)
            }
        })
    }

    private handleSubscribe(ws: WsWebSocket, data: Buffer | ArrayBuffer | Buffer[]) {
        const msg = JSON.parse(data.toString())
        if (msg.type !== 'subscribe') {
            this.sendTo(ws, {type: 'error', code: 1, message: 'expected subscribe message'})
            ws.close()
            return
        }

        if (!msg.contracts && !msg.receivers) {
            this.sendTo(ws, {
                type: 'error',
                code: 1,
                message: 'must specify contracts or receivers',
            })
            ws.close()
            return
        }

        this.sendTo(ws, {
            type: 'heartbeat',
            head_seq: String(this._headSeq),
            lib_seq: String(this._libSeq),
        })

        ws.on('message', (ackData) => {
            // handle ack messages silently
            JSON.parse(ackData.toString())
        })
    }

    sendAction(opts: {
        globalSeq: number
        blockNum?: number
        blockTime?: number
        contract: string
        action: string
        receiver?: string
        data?: Record<string, unknown>
        hexData?: string
        trxId?: string | null
    }): void {
        const msg: Record<string, unknown> = {
            type: 'action',
            global_seq: String(opts.globalSeq),
            block_num: opts.blockNum || 100,
            block_time: opts.blockTime || 1700000000,
            contract: opts.contract,
            action: opts.action,
            receiver: opts.receiver || opts.contract,
        }
        if (opts.data) {
            msg.data = opts.data
        }
        if (opts.hexData) {
            msg.hex_data = opts.hexData
        }
        if (opts.trxId !== null) {
            msg.trx_id =
                opts.trxId ?? '5b273364b825dfd58e7ac36e4014a24f1547cb5b1786a586af31c5a83daaa03b'
        }
        this.broadcast(msg)
    }

    sendCatchupComplete(): void {
        this.broadcast({
            type: 'catchup_complete',
            head_seq: String(this._headSeq),
            lib_seq: String(this._libSeq),
        })
    }

    sendError(code: number, message: string): void {
        this.broadcast({type: 'error', code, message})
    }

    broadcastHeartbeat(): void {
        this.broadcast({
            type: 'heartbeat',
            head_seq: String(this._headSeq),
            lib_seq: String(this._libSeq),
        })
    }

    setHead(headSeq: number, libSeq: number): void {
        this._headSeq = headSeq
        this._libSeq = libSeq
    }

    private broadcast(msg: Record<string, unknown>): void {
        const data = JSON.stringify(msg)
        for (const ws of this.clients) {
            if (ws.readyState === WsWebSocket.OPEN) {
                ws.send(data)
            }
        }
    }

    private sendTo(ws: WsWebSocket, msg: Record<string, unknown>): void {
        ws.send(JSON.stringify(msg))
    }

    close(): Promise<void> {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer)
            this.heartbeatTimer = null
        }
        return new Promise((resolve) => {
            if (!this.wss) {
                resolve()
                return
            }
            for (const ws of this.clients) {
                ws.close()
            }
            this.wss.close(() => {
                this.wss = null
                resolve()
            })
        })
    }
}
