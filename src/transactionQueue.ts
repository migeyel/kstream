import { TransactionPage } from "./transactionPage";
import { TransactionSet } from "./transactionSet";
import { ApiTransaction } from "./util";

/** A queue of contiguous transactions. */
export class TransactionQueue {
    /** The transactions in the queue, by ID. */
    private _transactions: LuaMap<number, ApiTransaction>;

    /** The transaction set hosting this queue. */
    private _set: TransactionSet;

    /** The next transaction ID to pop. */
    private _nextPopId: number;

    /**
     * The last transaction ID seen by the queue, or -1 when none have been seen. This
     * may include seen transactions that aren't in the filtered set (and thus aren't in
     * the queue itself).
     */
    private _lastSeenId: number;

    /**
     * Creates a new queue.
     * @param set The transaction set hosting this queue.
     * @param id A base transaction ID to start off of, or -1 for the first transaction.
     */
    public constructor(set: TransactionSet, id: number) {
        this._set = set;
        this._transactions = new LuaMap();
        this._nextPopId = id + 1;
        this._lastSeenId = id;
    }

    /**
     * Returns whether the queue is empty.
     * @returns whether the queue is empty.
     */
    public isEmpty(): boolean {
        let out = this._transactions.get(this._nextPopId);
        while (!out && this._nextPopId <= this._lastSeenId) {
            out = this._transactions.get(++this._nextPopId);
        }
        return !out;
    }

    /**
     * Pops a transaction from the queue.
     * @returns The popped transaction, or nil if the queue is empty.
     */
    public pop(): ApiTransaction | undefined {
        let out = this._transactions.get(this._nextPopId);
        while (!out && this._nextPopId <= this._lastSeenId) {
            out = this._transactions.get(++this._nextPopId);
        }
        if (out) { this._transactions.delete(out.id); }
        return out;
    }

    /**
     * Tries pushing a sole transaction into the queue. Only succeeds if the transaction
     * ID comes immediately after the last seen ID in the queue.
     * @param endpoint The endpoint that returned this transaction.
     * @param tx The transaction.
     * @returns Whether the push was successful.
     */
    public tryPushTransaction(tx: ApiTransaction): boolean {
        if (tx.id != this._lastSeenId + 1) { return false; }
        this._lastSeenId++;
        if (this._set.contains(tx) && this._nextPopId <= tx.id) {
            this._transactions.set(tx.id, tx);
        }
        return true;
    }

    /**
     * Tries pushing a transaction page.
     * @param page The transaction page to push.
     * @returns Whether the push was successful.
     */
    public tryPushPage(page: TransactionPage): boolean {
        // Refuse to push if we may miss a transaction from our set.
        if (!this._set.isSubset(page.set)) { return false; }

        // Either the page needs to contain a transaction we've already seen (so we
        // know there are no missing transactions in the middle), or it needs to be the
        // first page.
        if (!page.isStart() && page.page[0].id > this._lastSeenId + 1) {
            return false;
        }

        // Throw away transactions that we've already seen.
        let idx = 0;
        while (page.page[idx] && page.page[idx].id <= this._lastSeenId) { idx++; }

        // Copy over new transactions.
        for (const i of $range(idx, page.page.length - 1)) {
            const tx = page.page[i];
            if (this._set.contains(tx) && this._nextPopId <= tx.id) {
                this._transactions.set(tx.id, tx);
            }
            this._lastSeenId = tx.id;
        }

        return true;
    }

    /**
     * Updates the last seen transaction ID after being given evidence that there are
     * no transactions inside of an ID range.
     *
     * The evidence consists of the following facts:
     * 1. At a point in time A, there is some transaction that came after the last
     *    transaction seen by the queue on a superset of the queue's set.
     * 2. At a point in time B, there are no further transactions in the queue's
     *    transaction set.
     * 3. Point A happens earlier than B.
     *
     * These facts are provided by a pair of pages (`last`, `next`) which match these
     * properties.
     *
     * @param last The last page of the queue's set.
     * @param next A page from a superset which contains a transaction that came later,
     *             and was queried before `last` was.
     * @returns True if the last seen id was updated, or if the last page's last
     *          transaction is equal to the next page's last transaction.
     */
    public tryPushUnseen(last: TransactionPage, next: TransactionPage): boolean {
        if (!this._set.isSubset(last.set)) { return false; }
        if (!last.set.isSubset(next.set)) { return false; }
        if (!next.wasQueriedBefore(last)) { return false; }

        // The next page must have a transaction. This method is only called after a
        // socket receives a transaction so this is no issue.
        if (next.page.length == 0) { return false; }

        const nextId = next.page[next.page.length - 1].id;
        const lastId = last.page[last.page.length - 1]?.id || -1;

        // If last has unseen transactions, push them instead of rejecting.
        this.tryPushPage(last);

        if (!last.isEnd()) { return false; }
        if (lastId > this._lastSeenId) { return false; }
        if (lastId > nextId) { return false; }

        this._lastSeenId = math.max(this._lastSeenId, nextId);
        return true;
    }
}
