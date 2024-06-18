import { TransactionQueue } from "./transactionQueue";
import { TransactionSocket } from "./transactionSocket";
import { ApiTransaction } from "./util";
import { TransactionSet } from "./transactionSet";
import { TransactionPage, findTransaction } from "./transactionPage";
import { Mutex } from "./mutex";

/** How many transactions to fetch at once when populating the stream. */
const POPULATE_BLOCK_SIZE = 50;

/** A reliably delivered stream of transactions. */
export class TransactionStream {
    /** The Krist endpoint. */
    private _endpoint: string;

    /** The transaction socket. */
    private _socket: TransactionSocket;

    /** The last popped transaction ID. -1 when no transactions have been popped. */
    private _lastPoppedId: number;

    /**
     * The last popped offset, as of the time we checked it. Nil when there are no
     * transactions at or after the last popped ID.
     */
    private _lastPoppedOffset?: number;

    /** The transaction set we're operating on. */
    private _set: TransactionSet;

    /** Whether the socket is up since we last checked. */
    private _isUp: boolean;

    /** The tail end of the stream. */
    private _queue: TransactionQueue;

    /**
     * Set whenever we suspect the end of the queue has crossed the end of its set.
     * - Set when updateOffset() fails to fetch the transaction offset.
     * - Set when _populate() reaches the last page.
     * - Set when a socket push succeeds.
     * - Unset when the socket times out.
     */
    private _hasReachedTail = false;

    /**
     * Set whenever we suspect there is a hole between the last transaction seen by the
     * queue and the last transaction seen by the socket. It is only set if _reachedTail
     * is also set.
     * - Set when a socket push fails and _reachedTail is set.
     * - Unset when a socket push succeeds.
     * - Unset when _populate() reaches the last page.
     * - Unset after a hole fill operation.
     */
    private _hasTailHole = false;

    /** A mutex to hold to modify the stream. */
    public mutex = new Mutex();

    /** Updates the last pop offset. */
    private _updateOffset() {
        this._lastPoppedOffset = findTransaction(
            this._endpoint,
            this._set,
            this._lastPoppedId,
        );
        if (!this._lastPoppedOffset) { this._hasReachedTail = true; }
    }

    public constructor(endpoint: string, set: TransactionSet, lastPoppedId: number) {
        this._endpoint = endpoint;
        this._lastPoppedId = lastPoppedId;
        this._isUp = false;
        this._set = set;
        this._queue = new TransactionQueue(set, lastPoppedId);

        const transactionCb = (tx: ApiTransaction) => {
            const ok = this._queue.tryPushTransaction(tx);
            if (ok) {
                this._hasReachedTail = true;
                this._hasTailHole = false;
            } else if (this._hasReachedTail) {
                this._hasTailHole = true;
            }
        };

        const statusCb = (isUp: boolean) => {
            if (!isUp) { this._hasReachedTail = false; }
            this._isUp = isUp;
            os.queueEvent("kstream_stream_status");
        };

        this._socket = new TransactionSocket(endpoint, transactionCb, statusCb);
    }

    /**
     * Tries pushing the queue last seen past the last transaction id in our set and
     * into the last transaction id of all transactions. This is needed for it to start
     * accepting pushes from the socket.
     */
    private _fillTailHoles(): void {
        const bs = POPULATE_BLOCK_SIZE;
        const all = TransactionSet.all();
        const next = TransactionPage.fetch(this._endpoint, all, 0, bs, false);
        const last = TransactionPage.fetch(this._endpoint, this._set, 0, bs, false);
        this._queue.tryPushUnseen(last, next);
        this._hasTailHole = false;
    }

    /** Populates the queue with transactions up to POPULATE_BLOCK_SIZE. */
    private _populate(): void {
        // If the offset is past the end, update it and check if we can populate.
        if (!this._lastPoppedOffset) {
            this._updateOffset();
            if (!this._lastPoppedOffset) { return; }
        }

        // If our last transaction happens to be missing, lastPoppedOffset would point
        // to the *next* transaction in line, so we need to fetch one less (the
        // transaction before our last) so we can connect both pages.
        const offset = math.max(0, this._lastPoppedOffset - 1);
        const page = TransactionPage.fetch(
            this._endpoint,
            this._set,
            offset,
            POPULATE_BLOCK_SIZE,
            true,
        );

        const ok = this._queue.tryPushPage(page);
        if (ok) {
            this._hasReachedTail = page.isEnd();
            this._hasTailHole = false;
            return;
        }

        // Inserting the page failed because the last transaction was deleted, a
        // well as the previous one. Update the offset and try again.
        this._updateOffset();
        return this._populate();
    }

    public closeSocket(): void {
        return this._socket.close();
    }

    public isUp(): boolean {
        return this._isUp;
    }

    public listen(): void {
        return this._socket.listen();
    }

    /** Waits until a transaction is available to the stream. */
    public wait(): void {
        if (!this._queue.isEmpty()) { return; }

        // The queue has reached the end, so we need to wait for a tx on the socket.
        if (this._hasReachedTail) {
            if (this._hasTailHole) {
                this._fillTailHoles();
                return this.wait();
            } else {
                os.pullEvent("kstream_stream_status");
                return this.wait();
            }
        }

        // The queue hasn't reached the end, populate and keep popping.
        this._populate();
        return this.wait();

    }

    /** Pops a transaction from the stream and returns it. */
    public pop(): ApiTransaction {
        this.wait();
        const out = assert(this._queue.pop());
        if (this._lastPoppedOffset) { this._lastPoppedOffset++; }
        this._lastPoppedId = out.id;
        return out;
    }
}
