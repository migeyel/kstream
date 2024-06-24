import { ApiTransaction, parseTime } from "./util";

/** A Krist transacton, one of several types. */
export type Transaction = {
    /** The ID of this transaction. */
    id: number,

    /** The time this transaction this was made, as an ISO-8601 string. */
    time: string,

    /**
     * The time this transaction was made, as the number of non-leap seconds since
     * January 1, 1970 00:00:00 UTC
     */
    timestamp: number,
} & TransactionOperation;

type KnownTransactionOperation =
    | Transfer
    | MiningReward
    | NamePurchase
    | NameTransfer
    | NameRecordChange;

/** A transfer of currency from one addresses to another. */
export type Transfer = {
    type: "transfer",

    /** The sender of this transfer. */
    from: string,

    /** The recipient of this transfer. */
    to: string,

    /** The amount transferred. */
    value: number,

    /** Raw transaction metadata, or nil. */
    metadata?: string,

    /**
     * The name this transaction was sent to, without suffix, if it was sent to a name.
     *
     * Older software may send transfers by specifying the destination on the metadata
     * instead of the receiver's address field. These transfers will *not* set this
     * field.
     */
    sent_name?: string,

    /**
     * The metaname (part before the `"@"`) of the recipient of this transaction, if it
     * was sent to a name.
     *
     * See the `sent_name` field for compatibility remarks.
     */
    sent_metaname?: string,

    /**
     * The CommonMeta key-value records specified in transaction metadata. These include
     * only records of the form `"key=value"`.
     *
     * Parsing specifics:
     * - Parsing `"a=b=c"` yields `{"a": "b=c"}`.
     * - Parsing `"a=b;a=c"` yields `{"a": "c"}` and always chooses the last record.
     * - Parsing `"="` yields `{"": ""}`.
     * - Parsing `"a;b=c;d"` yields `{"b": "c"}`.
     * - Parsing `"Powered by = Kristify"` yields `{"Powered by ": " Kristify"}`
     */
    kv: LuaMap<string, string>,
}

/** A mining reward issued to an address. */
export type MiningReward = {
    type: "mined",

    /** The recipient of the mining reward. */
    to: string,

    /** The amount rewarded. */
    value: number,
}

/** The purchase of a name by an address. */
export type NamePurchase = {
    type: "name_purchase",

    /** The buyer of the name. */
    from: string,

    /** The name that has been purchased, without suffix. */
    name: string,

    /** The price that has been paid for the name. */
    value: number,
}

/** The transfer of a name from one address to another. */
export type NameTransfer = {
    type: "name_transfer",

    /** The previous owner of the name. */
    from: string,

    /** The new owner of the name. */
    to: string,

    /** The transferred name. */
    name: string,
}

/** The change of a name's associated A record to another value. */
export type NameRecordChange = {
    type: "name_a_record",

    /** The name which associated A record has been changed. */
    name: string,

    /** The A record's new contents, or nil if the record has been cleared. */
    metadata?: string,
}

export type Unknown = {
    type: Exclude<string, KnownTransactionOperation["type"]>,
    [value: string]: any,
};

export type TransactionOperation = KnownTransactionOperation | Unknown;

function parseMeta(m: string): LuaMap<string, string> {
    const kvMeta = new LuaMap<string, string>();
    for (const [record] of string.gmatch(m, "[^;]+")) {
        const [key, value] = string.match(record, "^([^=]*)=(.*)$");
        if (key != undefined && value != undefined) {
            kvMeta.set(key, value);
        }
    }
    return kvMeta;
}

export function parseApiTx(tx: ApiTransaction): Transaction {
    if (tx.type == "transfer") {
        return {
            id: tx.id,
            time: tx.time,
            timestamp: parseTime(tx.time),
            type: "transfer",
            from: assert(tx.from),
            to: assert(tx.to),
            value: tx.value,
            metadata: tx.metadata,
            sent_name: tx.sent_name,
            sent_metaname: tx.sent_metaname,
            kv: parseMeta(tx.metadata || ""),
        };
    } else if (tx.type == "mined") {
        return {
            id: tx.id,
            time: tx.time,
            timestamp: parseTime(tx.time),
            type: "mined",
            to: assert(tx.to),
            value: assert(tx.value),
        };
    } else if (tx.type == "name_purchase") {
        return {
            id: tx.id,
            time: tx.time,
            timestamp: parseTime(tx.time),
            type: "name_purchase",
            from: assert(tx.from),
            name: assert(tx.name),
            value: tx.value,
        };
    } else if (tx.type == "name_transfer") {
        return {
            id: tx.id,
            time: tx.time,
            timestamp: parseTime(tx.time),
            type: "name_transfer",
            from: assert(tx.from),
            to: assert(tx.to),
            name: assert(tx.name),
        };
    } else if (tx.type == "name_a_record") {
        return {
            id: tx.id,
            time: tx.time,
            timestamp: parseTime(tx.time),
            type: "name_a_record",
            name: assert(tx.name),
            metadata: tx.metadata,
        };
    } else {
        return {
            ...tx,
            timestamp: parseTime(tx.time),
        };
    }
}
