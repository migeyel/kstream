import { request } from "./http";
import { TransactionSet } from "./transactionSet";
import {
    ApiTransaction,
    ApiTransactionResponse,
    expectOk,
    findTxInSlice,
    parseJson,
} from "./util";

/** A page of contiguous transactions fetched from some Krist node. */
export class TransactionPage {
    /** The transaction page itself, ordered by transaction id. */
    public readonly page: ApiTransaction[];

    /** The transaction set containing this page. */
    public readonly set: TransactionSet;

    /** The total of transactions matching the filter returned by the node. */
    public readonly total: number;

    /** The page offset, in ascending order of id. */
    private _offset: number;

    /** The UTC timestamp from before the request was made. */
    private _timestampBefore: number;

    /** The UTC timestamp from after the request returned. */
    private _timestampAfter: number;

    private constructor(
        page: ApiTransaction[],
        set: TransactionSet,
        total: number,
        offset: number,
        timestampBefore: number,
        timestampAfter: number,
    ) {
        this.page = page;
        this.set = set;
        this.total = total;
        this._offset = offset;
        this._timestampBefore = timestampBefore;
        this._timestampAfter = timestampAfter;
    }

    /**
     * Fetches transactions.
     * @param endpoint The krist node to fetch from.
     * @param set The transaction set to fetch from.
     * @param offset The offset into the transaction list.
     * @param limit The limit on the amount of fetched transactions.
     * @param ascending Whether to use ascending or descending order.
     */
    public static fetch(
        endpoint: string,
        set: TransactionSet,
        offset: number,
        limit: number,
        ascending: boolean,
    ): TransactionPage {
        assert(offset >= 0 && offset < 2 ** 48 && offset % 1 == 0);
        assert(limit > 0 && limit <= 1000 && limit % 1 == 0);

        const url = endpoint + set.getQuery(offset, limit, ascending);
        const timestampBefore = os.epoch("utc");
        const handle = request({ method: "GET", url, body: "" })!;
        assert(handle.ok, handle.msg);
        const s = handle.h.readAll() || "";
        const timestampAfter = os.epoch("utc");

        const obj = parseJson(s, url);
        const res = expectOk<ApiTransactionResponse>(obj);
        const page = res.transactions.toSorted((a, b) => a.id - b.id);
        const ascOffset = ascending ? offset : res.total - offset - page.length;

        return new TransactionPage(
            page,
            set,
            res.total,
            ascOffset,
            timestampBefore,
            timestampAfter,
        );
    }

    /**
     * Whether this page's state view is guaranteed to have preceeded from another.
     *
     * This assumes that `os.epoch("utc")` is monotonically increasing.
     *
     * @param other The other page to compare to.
     */
    public wasQueriedBefore(other: TransactionPage): boolean {
        return this._timestampAfter < other._timestampBefore;
    }

    /**
     * If true, then this page is either empty or contains the first transaction on the
     * set.
     */
    public isStart(): boolean {
        return this._offset <= 0;
    }

    /**
     * If true, then this page is either empty or contains the last transaction on the
     * set.
     */
    public isEnd(): boolean {
        return this._offset + this.page.length >= this.total;
    }
}

const TX_FETCH_LIMIT = 50;

/**
 * Finds a transaction offset at or after the given ID.
 * @param endpoint The Krist endpoint.
 * @param id The transaction ID, or -1 for finding the first transaction.
 * @returns The offset, or nil if the transaction ID comes after the last transaction.
 */
export function findTransaction(
    endpoint: string,
    set: TransactionSet,
    id: number,
): number | undefined {
    const last = TransactionPage.fetch(endpoint, set, 0, TX_FETCH_LIMIT, false);

    if (id == -1) {
        // Return offset 0 if there's a transaction there.
        return last.total > 0 ? 0 : undefined;
    }

    // Check if all transactions fit in a single slice.
    if (last.total <= last.page.length) {
        return findTxInSlice(id, last.page);
    }

    // Check if the transaction is in the last slice.
    if (last.page[0].id <= id) {
        const out = findTxInSlice(id, last.page);
        return out && out + last.total - last.page.length - 1;
    }

    // Perform a search to find the transaction's offset.
    let minK = -1;
    let minV = 0;
    let maxK = last.total - 1;
    let maxV = last.page[last.page.length - 1].id;
    let iter = 0;
    while (minK + 1 < maxK) {
        let midK;
        if (++iter <= 3) {
            // Up to 3 rounds of interpolation search
            midK = minK + (id - minV) * (maxK - minK) / (maxV - minV);
        } else {
            // Binary search
            midK = (minK + maxK) / 2;
        }

        midK = math.floor(midK + 0.5);
        midK = math.min(midK, maxK - 1);
        midK = math.max(midK, minK + 1);

        const midTx = TransactionPage.fetch(endpoint, set, midK, 1, true).page[0];
        if (!midTx) { return findTransaction(endpoint, set, id); }

        const midV = midTx.id;
        if (midV < id) {
            if (midV < minV) { return findTransaction(endpoint, set, id); }
            minK = midK;
            minV = midV;
        } else {
            if (midV > maxV) { return findTransaction(endpoint, set, id); }
            maxK = midK;
            maxV = midV;
        }
    }

    // Because I don't trust the search completely, validate its results.
    const edges = TransactionPage.fetch(endpoint, set, minK, 2, true);
    if (edges.page[0].id >= id || edges.page[1].id < id) {
        return findTransaction(endpoint, set, id);
    }

    return maxK;
}
