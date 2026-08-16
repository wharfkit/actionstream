import {Checksum256, Name, NameType, UInt64, UInt64Type} from '@wharfkit/antelope'

export interface ActionStreamFilter {
    contracts?: NameType[]
    receivers?: NameType[]
    actions?: NameType[]
}

export interface ActionStreamOptions {
    startSeq?: UInt64Type | 'head'
    decode?: boolean
    reconnectDelay?: number
    reconnectMaxDelay?: number
    ackInterval?: number
    queueSize?: number
    healthyThreshold?: number
}

export interface StreamOverflow {
    droppedFrom: UInt64
    resumeSeq: UInt64
    queueSize: number
    overflowCount: number
}

export interface StreamGap {
    expected: number
    received: number
    resumeSeq: UInt64
}

export interface StreamAction {
    globalSeq: UInt64
    blockNum: number
    blockTime: number
    contract: Name
    action: Name
    receiver: Name
    trxId: Checksum256
    hexData?: string
    data?: Record<string, unknown>
}

export interface StreamState {
    headSeq: UInt64
    libSeq: UInt64
}

export enum ErrorCode {
    InvalidRequest = 1,
    ServerSyncing = 2,
    MaxClients = 3,
    NoActions = 4,
    DataInconsistent = 5,
    ResyncRequired = 6,
}

export interface StreamError {
    code: number
    message: string
}

export interface WsSubscribeMessage {
    type: 'subscribe'
    contracts?: string[]
    receivers?: string[]
    actions?: string[]
    start_seq?: string
    decode?: boolean
}

export interface WsActionMessage {
    type: 'action'
    global_seq: string
    block_num: number
    block_time: number
    contract: string
    action: string
    receiver: string
    trx_id?: string
    hex_data?: string
    data?: Record<string, unknown>
    sub_seq?: number
}

export interface WsHeartbeatMessage {
    type: 'heartbeat'
    head_seq: string
    lib_seq: string
}

export interface WsCatchupCompleteMessage {
    type: 'catchup_complete'
    head_seq: string
    lib_seq: string
}

export interface WsErrorMessage {
    type: 'error'
    code: number
    message: string
}

export interface WsAckMessage {
    type: 'ack'
    seq: string
}

export type WsServerMessage =
    | WsActionMessage
    | WsHeartbeatMessage
    | WsCatchupCompleteMessage
    | WsErrorMessage
