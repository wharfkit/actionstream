import {Name, UInt64} from '@wharfkit/antelope'

import {
    ActionStreamFilter,
    ActionStreamOptions,
    StreamAction,
    StreamState,
    WsAckMessage,
    WsServerMessage,
    WsSubscribeMessage,
} from './types'

const DEFAULT_RECONNECT_DELAY = 1000
const DEFAULT_RECONNECT_MAX_DELAY = 30000
const DEFAULT_ACK_INTERVAL = 1000
const DEFAULT_QUEUE_SIZE = 1000

export class ActionStreamClient {
    private url: string
    private filter: ActionStreamFilter
    private decode: boolean
    private reconnectDelay: number
    private reconnectMaxDelay: number
    private ackInterval: number

    private ws: WebSocket | null = null
    private currentSeq: UInt64
    private lastAcked: UInt64 = UInt64.from(0)
    private closed = false
    private _connected = false
    private _catchupComplete = false
    private _headSeq: UInt64 = UInt64.from(0)
    private _libSeq: UInt64 = UInt64.from(0)

    private queue: StreamAction[] = []
    private waitingResolve: ((action: StreamAction) => void) | null = null
    private waitingReject: ((err: Error) => void) | null = null
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null
    private currentBackoff: number

    onHeartbeat?: (state: StreamState) => void
    onCatchupComplete?: (state: StreamState) => void
    onError?: (code: number, message: string) => void
    onConnect?: () => void
    onDisconnect?: () => void

    constructor(url: string, filter: ActionStreamFilter, options?: ActionStreamOptions) {
        this.url = url
        this.filter = filter
        this.decode = options && options.decode !== undefined ? options.decode : true
        this.reconnectDelay =
            options && options.reconnectDelay !== undefined
                ? options.reconnectDelay
                : DEFAULT_RECONNECT_DELAY
        this.reconnectMaxDelay =
            options && options.reconnectMaxDelay !== undefined
                ? options.reconnectMaxDelay
                : DEFAULT_RECONNECT_MAX_DELAY
        this.ackInterval =
            options && options.ackInterval !== undefined
                ? options.ackInterval
                : DEFAULT_ACK_INTERVAL
        this.currentSeq =
            options && options.startSeq !== undefined
                ? UInt64.from(options.startSeq)
                : UInt64.from(0)
        this.currentBackoff = this.reconnectDelay
    }

    get headSeq(): UInt64 {
        return this._headSeq
    }

    get libSeq(): UInt64 {
        return this._libSeq
    }

    get connected(): boolean {
        return this._connected
    }

    get catchupComplete(): boolean {
        return this._catchupComplete
    }

    connect(): void {
        if (this.closed) {
            throw new Error('Client is closed')
        }
        this.dial()
    }

    close(): void {
        if (this.closed) {
            return
        }
        this.closed = true
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer)
            this.reconnectTimer = null
        }
        const wasConnected = this._connected
        if (this.ws) {
            this.ws.onclose = null
            this.ws.close()
            this.ws = null
        }
        this._connected = false
        if (wasConnected && this.onDisconnect) {
            this.onDisconnect()
        }
        if (this.waitingReject) {
            this.waitingReject(new Error('Client closed'))
            this.waitingResolve = null
            this.waitingReject = null
        }
    }

    next(): Promise<StreamAction> {
        if (this.closed) {
            return Promise.reject(new Error('Client closed'))
        }
        if (this.queue.length > 0) {
            const action = this.queue.shift()!
            this.trackSeq(action)
            return Promise.resolve(action)
        }
        return new Promise<StreamAction>((resolve, reject) => {
            this.waitingResolve = resolve
            this.waitingReject = reject
        })
    }

    nextWithTimeout(timeoutMs: number): Promise<StreamAction | null> {
        if (this.closed) {
            return Promise.reject(new Error('Client closed'))
        }
        if (this.queue.length > 0) {
            const action = this.queue.shift()!
            this.trackSeq(action)
            return Promise.resolve(action)
        }
        return new Promise<StreamAction | null>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.waitingResolve = null
                this.waitingReject = null
                resolve(null)
            }, timeoutMs)
            this.waitingResolve = (action: StreamAction) => {
                clearTimeout(timer)
                resolve(action)
            }
            this.waitingReject = (err: Error) => {
                clearTimeout(timer)
                reject(err)
            }
        })
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<StreamAction> {
        while (!this.closed) {
            try {
                const action = await this.next()
                yield action
            } catch {
                return
            }
        }
    }

    private dial(): void {
        const ws = new WebSocket(this.url)
        this.ws = ws

        ws.onopen = () => {
            this.sendSubscribe()
        }

        ws.onmessage = (event: MessageEvent) => {
            this.handleMessage(event.data)
        }

        ws.onerror = () => {
            // onclose will fire after onerror
        }

        ws.onclose = () => {
            const wasConnected = this._connected
            this._connected = false
            if (wasConnected && this.onDisconnect) {
                this.onDisconnect()
            }
            if (!this.closed) {
                this.scheduleReconnect()
            }
        }
    }

    private sendSubscribe(): void {
        if (!this.ws) {
            return
        }

        const msg: WsSubscribeMessage = {
            type: 'subscribe',
        }

        if (this.filter.contracts && this.filter.contracts.length > 0) {
            msg.contracts = this.filter.contracts.map((c) => String(Name.from(c)))
        }
        if (this.filter.receivers && this.filter.receivers.length > 0) {
            msg.receivers = this.filter.receivers.map((r) => String(Name.from(r)))
        }
        if (this.filter.actions && this.filter.actions.length > 0) {
            msg.actions = this.filter.actions.map((a) => String(Name.from(a)))
        }

        if (this.currentSeq.gt(UInt64.from(0))) {
            msg.start_seq = this.currentSeq.toString()
        }

        msg.decode = this.decode

        this.ws.send(JSON.stringify(msg))
        this._connected = true
        this.currentBackoff = this.reconnectDelay
        if (this.onConnect) {
            this.onConnect()
        }
    }

    private handleMessage(data: string | ArrayBuffer | Blob): void {
        if (typeof data !== 'string') {
            return
        }

        let msg: WsServerMessage
        try {
            msg = JSON.parse(data) as WsServerMessage
        } catch {
            return
        }

        switch (msg.type) {
            case 'action':
                this.handleAction(msg)
                break
            case 'heartbeat':
                this.handleHeartbeat(msg)
                break
            case 'catchup_complete':
                this.handleCatchupComplete(msg)
                break
            case 'error':
                this.handleError(msg)
                break
        }
    }

    private handleAction(msg: {
        global_seq: string
        block_num: number
        block_time: number
        contract: string
        action: string
        receiver: string
        hex_data?: string
        data?: Record<string, unknown>
    }): void {
        const action: StreamAction = {
            globalSeq: UInt64.from(msg.global_seq),
            blockNum: msg.block_num,
            blockTime: msg.block_time,
            contract: Name.from(msg.contract),
            action: Name.from(msg.action),
            receiver: Name.from(msg.receiver),
        }
        if (msg.hex_data) {
            action.hexData = msg.hex_data
        }
        if (msg.data) {
            action.data = msg.data
        }

        if (this.waitingResolve) {
            const resolve = this.waitingResolve
            this.waitingResolve = null
            this.waitingReject = null
            this.trackSeq(action)
            resolve(action)
        } else if (this.queue.length < DEFAULT_QUEUE_SIZE) {
            this.queue.push(action)
        }
    }

    private handleHeartbeat(msg: {head_seq: string; lib_seq: string}): void {
        this._headSeq = UInt64.from(msg.head_seq)
        this._libSeq = UInt64.from(msg.lib_seq)
        if (this.onHeartbeat) {
            this.onHeartbeat({headSeq: this._headSeq, libSeq: this._libSeq})
        }
    }

    private handleCatchupComplete(msg: {head_seq: string; lib_seq: string}): void {
        this._headSeq = UInt64.from(msg.head_seq)
        this._libSeq = UInt64.from(msg.lib_seq)
        this._catchupComplete = true
        if (this.onCatchupComplete) {
            this.onCatchupComplete({headSeq: this._headSeq, libSeq: this._libSeq})
        }
    }

    private handleError(msg: {code: number; message: string}): void {
        if (this.onError) {
            this.onError(msg.code, msg.message)
        }
    }

    private trackSeq(action: StreamAction): void {
        this.currentSeq = action.globalSeq.adding(1).cast(UInt64)

        if (this.ackInterval > 0) {
            const diff = action.globalSeq.subtracting(this.lastAcked)
            if (diff.gte(UInt64.from(this.ackInterval))) {
                this.lastAcked = action.globalSeq
                this.sendAck(action.globalSeq)
            }
        }
    }

    private sendAck(seq: UInt64): void {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            return
        }
        const msg: WsAckMessage = {
            type: 'ack',
            seq: seq.toString(),
        }
        this.ws.send(JSON.stringify(msg))
    }

    private scheduleReconnect(): void {
        const delay = this.currentBackoff
        this.currentBackoff = Math.min(this.currentBackoff * 2, this.reconnectMaxDelay)
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null
            if (this.closed) {
                return
            }
            this.dial()
        }, delay)
    }
}
