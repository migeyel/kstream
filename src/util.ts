/** A response success. */
export type ApiSuccess = {
    ok: true,
};

/** A response error. */
export type ApiError = {
    ok: false,
    error: string,
    message?: string,
};

/** A sent transaction. */
export type ApiTransaction = {
    /** The ID of this transaction. */
    id: number,

    /** The sender of this transaction. */
    from?: string,

    /**
     * The recipient of this transaction. This may be "name" if the transaction
     * was a name purchase, or "a" if it was a name's data change.
     */
    to: string,

    /**
     * The amount of Krist transferred in this transaction. Can be 0, notably if
     * the transaction was a name's data change.
     */
    value: number,

    /** The time this transaction this was made, as an ISO-8601 string. */
    time: string,

    /** The name associated with this transaction, without the .kst suffix. */
    name?: string,

    /** Transaction metadata. */
    metadata?: string,

    /**
     * The metaname (part before the "@") of the recipient of this transaction,
     * if it was sent to a name.
     */
    sent_metaname?: string,

    /**
     * The name this transaction was sent to, without the .kst suffix, if it was
     * sent to a name.
     */
    sent_name?: string,

    /**
     * The type of this transaction. May be "mined", "transfer",
     * "name_purchase", "name_a_record", or "name_transfer". Note that
     * name_a_record refers to a name's data changing.
     */
    type: string,
};

/** The response from a transaction query. */
export type ApiTransactionResponse = {
    ok: true,

    /** The count of results. */
    count: number,

    /** The total amount of transactions. */
    total: number,

    /** The ordered transaction list. */
    transactions: ApiTransaction[],
};

/** The response from an address query. */
export type ApiAddressResponse = {
    ok: true,
    address: {
        address: string,
        balance: number,
        totalin: number,
        totalout: number,
        firstseen: string,
    },
}

/** The interesting attributes of an API extended search response. */
export type ApiSearchResponse = {
    ok: true,
    matches: {
        transactions: {
            metadata: number,
        },
    },
};

export function hex(str: string): string {
    return string.format(string.rep("%02x", str.length), ...string.byte(str, 1, -1));
}

/**
 * Selects between a standard timeout and an optional deadline.
 * @param timeout A default timeout for an operation.
 * @param deadline An optional deadline that may override the timeout.
 * @returns A timeout value that takes the deadline into account.
 */
export function timeoutOrDeadline(timeout: number, deadline?: number): number {
    return math.max(0, math.min(timeout, (deadline || math.huge) - os.clock()));
}

export function expectOk<T extends { ok: true }>(response: T | ApiError): T {
    if (response.ok) {
        return response;
    } else {
        throw new Error(string.format(
            "Krist node returned an unexpected error: %q",
            response.message || response.error || "unknown error",
        ));
    }
}

const DAYS = [
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
    [366, 397, 425, 456, 486, 517, 547, 578, 609, 639, 670, 700],
    [731, 762, 790, 821, 851, 882, 912, 943, 974, 1004, 1035, 1065],
    [1096, 1127, 1155, 1186, 1216, 1247, 1277, 1308, 1339, 1369, 1400, 1430],
];

export function parseTime(timestamp: string) {
    const [sYear, sMonth, sDay, sHour, sMinute, sSecond] = string.match(
        timestamp,
        "^2(%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d%.?%d*)Z$",
    );

    const year = tonumber(sYear)!;
    const month = tonumber(sMonth)! - 1;
    const day = tonumber(sDay)! - 1;
    const hour = tonumber(sHour)!;
    const minute = tonumber(sMinute)!;
    const second = tonumber(sSecond)!;

    const days = year / 4 * (365 * 4 + 1) + DAYS[year % 4][month] + day;
    const hours = days * 24 + hour;
    const minutes = hours * 60 + minute;
    const seconds = minutes * 60 + second;

    return seconds + 946684800;
}

/**
 * Finds a transaction offset at or after the given ID on a slice.
 * @param id The ID to look for.
 * @param slice A contiguous set of transactions.
 * @returns The index at the slice which contains the satisfying transaction.
 */
export function findTxInSlice(id: number, slice: ApiTransaction[]): number | undefined {
    let out;
    let tgt = math.huge;
    for (const [i, tx] of ipairs(slice)) {
        if (tx.id >= id && tx.id < tgt) {
            out = i;
            tgt = tx.id;
        }
    }
    return out;
}

/** Copies a serializable object. */
export function copy<T>(obj: T): T {
    return textutils.unserialize(textutils.serialize(obj));
}

/** Parses JSON and prints the URL on error. */
export function parseJson(s: string, url: string): any {
    const [out, err] = [textutils.unserializeJSON(s)];
    if (err) { throw "JSON error URL: " + url + "\n" + tostring(err); }
    return out;
}
