import {Checksum256, Name, UInt64} from '@wharfkit/antelope'

import {
    ActionStreamFilter,
    ActionStreamOptions,
    ErrorCode,
    StreamAction,
    StreamGap,
    StreamOverflow,
    StreamState,
    WsAckMessage,
    WsActionMessage,
    WsServerMessage,
    WsSubscribeMessage,
} from './types'

const DEFAULT_RECONNECT_DELAY = 1000
const DEFAULT_RECONNECT_MAX_DELAY = 30000
const DEFAULT_ACK_INTERVAL = 1000
const DEFAULT_QUEUE_SIZE = 1000
const DEFAULT_HEALTHY_THRESHOLD = 10000

const START_AT_HEAD_SENTINEL = UInt64.from(UInt64.max)

export class ActionStreamClient {
    private url: string
    private filter: ActionStreamFilter
    private decode: boolean
    private reconnectDelay: number
    private reconnectMaxDelay: number
    private ackInterval: number
    private queueSize: number
    private healthyThreshold: number
    private startAtHead: boolean

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
    private readonly ackIntervalU64: UInt64
    private hasAcked = false
    private catchupCompleteAt: number | null = null
    private _overflowCount = 0
    private expectedSubSeq: number | null = 1

    onHeartbeat?: (state: StreamState) => void
    onCatchupComplete?: (state: StreamState) => void
    onError?: (code: number, message: string) => void
    onConnect?: () => void
    onDisconnect?: () => void
    onOverflow?: (overflow: StreamOverflow) => void
    onGap?: (gap: StreamGap) => void

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
        this.queueSize =
            options && options.queueSize !== undefined ? options.queueSize : DEFAULT_QUEUE_SIZE
        this.healthyThreshold =
            options && options.healthyThreshold !== undefined
                ? options.healthyThreshold
                : DEFAULT_HEALTHY_THRESHOLD
        this.startAtHead = options !== undefined && options.startSeq === 'head'
        this.currentSeq =
            options && options.startSeq !== undefined && !this.startAtHead
                ? UInt64.from(options.startSeq)
                : UInt64.from(0)
        this.currentBackoff = this.reconnectDelay
        this.ackIntervalU64 = UInt64.from(this.ackInterval)
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

    get overflowCount(): number {
        return this._overflowCount
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
        const queued = this.takeQueued()
        if (queued) {
            return Promise.resolve(queued)
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
        const queued = this.takeQueued()
        if (queued) {
            return Promise.resolve(queued)
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

    private takeQueued(): StreamAction | null {
        if (this.queue.length === 0) {
            return null
        }
        const action = this.queue.shift()!
        this.ackSeq(action)
        return action
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

    // resetConnectionState clears every field whose lifetime is one socket.
    private resetConnectionState(): void {
        this.catchupCompleteAt = null
        this.expectedSubSeq = 1
    }

    private dial(): void {
        const ws = new WebSocket(this.url)
        this.ws = ws
        this.resetConnectionState()

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
                if (this.wasHealthy()) {
                    this.currentBackoff = this.reconnectDelay
                }
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
        } else if (this.startAtHead) {
            msg.start_seq = START_AT_HEAD_SENTINEL.toString()
        }

        msg.decode = this.decode

        this.ws.send(JSON.stringify(msg))
        this._connected = true
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

    private handleAction(msg: WsActionMessage): void {
        if (msg.sub_seq === undefined) {
            this.expectedSubSeq = null
        } else if (this.expectedSubSeq !== null) {
            if (msg.sub_seq !== this.expectedSubSeq) {
                this.handleGap(msg.sub_seq)
                return
            }
            this.expectedSubSeq = msg.sub_seq + 1
        }
        if (!msg.trx_id) {
            if (this.onError) {
                this.onError(
                    ErrorCode.DataInconsistent,
                    `Action ${msg.global_seq} arrived without a trx_id`
                )
            }
            return
        }
        const action: StreamAction = {
            globalSeq: UInt64.from(msg.global_seq),
            blockNum: msg.block_num,
            blockTime: msg.block_time,
            contract: Name.from(msg.contract),
            action: Name.from(msg.action),
            receiver: Name.from(msg.receiver),
            trxId: Checksum256.from(msg.trx_id),
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
            this.acceptSeq(action)
            this.ackSeq(action)
            resolve(action)
        } else if (this.queue.length < this.queueSize) {
            this.queue.push(action)
            this.acceptSeq(action)
        } else {
            this.handleOverflow(action)
        }
    }

    private handleOverflow(action: StreamAction): void {
        this._overflowCount++
        if (this.onOverflow) {
            this.onOverflow({
                droppedFrom: action.globalSeq,
                resumeSeq: this.currentSeq,
                queueSize: this.queue.length,
                overflowCount: this._overflowCount,
            })
        }
        this.resubscribe()
    }

    private handleGap(received: number): void {
        const expected = this.expectedSubSeq!
        if (this.onGap) {
            this.onGap({expected, received, resumeSeq: this.currentSeq})
        }
        this.resubscribe()
    }

    private resubscribe(): void {
        if (!this.ws) {
            return
        }
        const ws = this.ws
        this.ws = null
        // Leave onclose wired: it owns the disconnect notice and the reconnect schedule.
        ws.onmessage = null
        ws.close()
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
        this.catchupCompleteAt = Date.now()
        if (this.onCatchupComplete) {
            this.onCatchupComplete({headSeq: this._headSeq, libSeq: this._libSeq})
        }
    }

    private handleError(msg: {code: number; message: string}): void {
        if (this.onError) {
            this.onError(msg.code, msg.message)
        }
    }

    private acceptSeq(action: StreamAction): void {
        if (action.globalSeq.lt(this.currentSeq)) {
            return
        }
        this.currentSeq = action.globalSeq.adding(1).cast(UInt64)
    }

    private ackSeq(action: StreamAction): void {
        if (this.ackInterval <= 0) {
            return
        }
        if (this.hasAcked && action.globalSeq.lte(this.lastAcked)) {
            return
        }
        const diff = action.globalSeq.subtracting(this.lastAcked)
        if (diff.gte(this.ackIntervalU64)) {
            this.lastAcked = action.globalSeq
            this.hasAcked = true
            this.sendAck(action.globalSeq)
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

    private wasHealthy(): boolean {
        if (this.catchupCompleteAt === null) {
            return false
        }
        return Date.now() - this.catchupCompleteAt >= this.healthyThreshold
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
