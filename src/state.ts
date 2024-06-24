import { HeldMutex, Mutex } from "./mutex";
import { TransactionPage } from "./transactionPage";
import { TransactionSet } from "./transactionSet";
import { ApiTransaction } from "./util";

const PATH = "stream.ltn";
const PATH_MOD = "stream.mod.ltn";
const PATH_NEW = "stream.new.ltn";

export type StoredState = {
    /** The Krist endpoint. */
    endpoint: string,

    /** Whether the transaction set includes mined transactions. */
    includeMined: boolean,

    /** An address, if we're narrowing transactions down. */
    address?: string,

    /** The last transaction ID popped from the stream, or -1 if none. */
    lastPoppedId: number,

    /** Box state that has been committed. */
    committed: Boxes,

    /** Box state that has been prepared for a commit. */
    prepared?: Boxes,
};

/** An outgoing transaction. */
export type OutgoingTransaction = {
    /** The transaction amount. */
    amount: number,

    /** The transaction receiver. */
    to: string,

    /** The sender's private key. */
    privateKey: string,

    /** Transaction CommonMeta key-value pairs. */
    meta: LuaMap<string, string>,

    /** Custom user data. */
    ud: any,
};

export enum OutboxStatus {
    /** The transaction is definitely pending and hasn't been sent. */
    PENDING = "pending",

    /** We don't know whether the transaction has been sent or not. */
    UNKNOWN = "unknown",

    /** The transaction has definitely been sent. */
    SENT = "sent",
}

export type OutboxEntry = {
    /** The transaction status. */
    status: OutboxStatus,

    /** The outgoing transaction. */
    transaction: OutgoingTransaction,

    /** An UUID to report back to the user. */
    id: string,

    /** An UUID for tracking completion. */
    ref: string,
};

export type Boxes = {
    /** A number for tracking distributed commits. */
    revision: number,

    /** Incoming transaction. */
    inbox?: ApiTransaction,

    /** Outgoing transactions. */
    outbox: OutboxEntry[],
};

/** Manages reading and writing the internal state of the program. */
export class State {
    private _dir: string;
    public state: StoredState;
    private _mutex = new Mutex();

    private constructor(dir: string, state: StoredState) {
        this._dir = dir;
        this.state = state;
    }

    /** Acquires the mutex and checks for inconsistent prepared state. */
    public lock(): HeldMutex {
        const held = this._mutex.lock();
        if (this.state.prepared) { error("stream has prepared state"); }
        return held;
    }

    public tryLock(timeout?: number): HeldMutex | undefined {
        const held = this._mutex.tryLock(timeout);
        if (!held) { return; }
        if (this.state.prepared) { error("stream has prepared state"); }
        return held;
    }

    /** Opens the state at a given directory. */
    public static open(dir: string, revision?: number): State {
        if (!fs.isDir(dir)) {
            error("not a directory: " + dir, 2);
        }

        const path = fs.combine(dir, PATH);
        const pathMod = fs.combine(dir, PATH_MOD);
        const pathNew = fs.combine(dir, PATH_NEW);

        fs.delete(pathNew);

        if (fs.exists(path)) {
            fs.delete(pathMod);
        } else if (fs.exists(pathMod)) {
            fs.move(pathMod, path);
        } else {
            error("not a valid state directory: " + dir, 2);
        }

        const [f, err] = fs.open(path, "rb");
        if (!f) { error("couldn't open state at " + dir + ": " + err, 2); }

        const data = f.readAll() || "";
        f.close();

        const state: StoredState = assert(textutils.unserialize(data));

        // If the revision points to the prepared sub-state, replace it. Otherwise, keep
        // using the committed and discard the prepared state.
        if (state.prepared && state.prepared.revision == revision) {
            state.committed = state.prepared;
            state.prepared = undefined;
        } else {
            state.prepared = undefined;
        }

        const out = new State(dir, state);
        out.commit();
        return out;
    }

    /**
     * Creates a new state on a directory. This method phones the endpoint to fetch the
     * last transaction ID so we don't start from the very first transaction.
     */
    public static create(
        dir: string,
        endpoint: string,
        includeMined: boolean,
        address?: string,
    ) {
        fs.makeDir(dir);

        const path = fs.combine(dir, PATH);
        const pathNew = fs.combine(dir, PATH_NEW);

        const all = TransactionSet.all();
        const last = TransactionPage.fetch(endpoint, all, 0, 1, false);
        const state = <StoredState>{
            endpoint,
            lastPoppedId: last.page[0]?.id || -1,
            includeMined: !!includeMined,
            address,
            committed: { revision: 0, outbox: [] },
        };

        const [fOpt, err] = fs.open(pathNew, "wb");
        const [f] = assert(fOpt, err);
        f.write(textutils.serialize(state));
        f.close();

        fs.move(pathNew, path);
    }

    /** Writes the state to disk. */
    public commit() {
        const path = fs.combine(this._dir, PATH);
        const pathMod = fs.combine(this._dir, PATH_MOD);

        const [fOpt, err] = fs.open(pathMod, "wb");
        const [f] = assert(fOpt, err);
        f.write(textutils.serialize(this.state));
        f.close();

        fs.delete(path);
        fs.move(pathMod, path);
    }
}
