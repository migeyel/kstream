import * as expect from "cc/expect";
import { OutboxEntry, OutboxStatus, OutgoingTransaction, State } from "./state";
import { TransactionSet } from "./transactionSet";
import { TransactionStream } from "./transactionStream";
import { InnerHookContext, HookContext } from "./hookContext";
import { request } from "./http";
import { ApiAddressResponse, ApiError, ApiSearchResponse, expectOk, parseJson, timeoutOrDeadline } from "./util";
import { HeldMutex, Mutex } from "./mutex";
import { parseApiTx, Transaction } from "./transaction";

const MAX_TX_TIMEOUT = 10;

/**
 * A disk-backed persistent stream of transactions.
 * 
 * Streams' states are stored in a directory. You can create a new stream using
 * {@link create()} and open an existing stream using {@link open()}.
 * 
 * ### Stream Hooks
 * Stream interaction is done using stream hooks. For example, you can define the
 * `onTransaction` hook for incoming transactions:
 * 
 * ```lua
 * local stream = kstream.Stream.open("/stream")
 * function stream.onTransaction(ctx, tx)
 *     if tx.type ~= "transfer" then return end
 *     print("A transfer is happening from", tx.from, "to", tx.to)
 * end
 * ```
 * 
 * Hooks are reliable in that they will run for every transaction in the same order they
 * were sent. If a transaction was sent with the computer turned off, the stream will
 * backtrack and start calling `onTransaction` from where it left off. You can check for
 * old transactions by comparing `tx.timestamp` against `os.epoch("utc")`.
 * 
 * Hooks are always guaranteed to run to completion *at least once*. If a hook errors or
 * the computer gets shut down, it will run again at the next opportuinity it gets. If
 * that is undesirable (say, if you're dropping an item or performing a side-effect for
 * an incoming transaction), then you can instead define the `afterCommit` callback:
 * 
 * ```lua
 * function stream.onTransaction(ctx, tx)
 *     if tx.type ~= "transfer" then
 *         -- ctx.afterCommit is nil so it doesn't get run here.
 *         return
 *     end
 * 
 *     function ctx.afterCommit()
 *         print("A transfer has happened from", tx.from, "to", tx.to)
 *     end
 * end
 * ```
 * 
 * Unlike `onTransaction`, `afterCommit` is guaranteed to run *at most once*, after the
 * main hook commits. If `afterCommit` errors or the computer gets shut down, it will
 * not run again.
 * 
 * Hooks are also equipped with a hook context. The context can be used to send out
 * transactions atomically: either the hook completes and enqueues all its transactions,
 * or it fails and enqueues none of them.
 * 
 * For example, the hook below is guaranteed to refund all incoming refundable
 * transactions exactly once, no matter how many times the computer shuts down:
 * 
 * ```lua
 * function stream.onTransaction(ctx, tx)
 *     local refund = kstream.makeRefund(pkey, address, tx)
 *     if refund then ctx:enqueueSend(refund) end
 * end
 * ```
 */
export class Stream {
    /** The main state. */
    private _state: State;

    /** The underlying in-memory stream. */
    private _stream: TransactionStream;

    /** A mutex for run() to hold. */
    private _routineMutex = new Mutex();

    /** A UUID for identifying the stream. */
    public readonly id: string;

    /** A hook to run on every new incoming transaction. */
    public onTransaction?: (this: void, ctx: HookContext, tx: Transaction) => void;

    /** A hook to run on every successful outgoing transaction. */
    public onSendSuccess?: (
        this: void,
        ctx: HookContext,
        tx: OutgoingTransaction,
        uuid: string,
    ) => void;

    /** A hook to run on every failed outgoing transaction. */
    public onSendFailure?: (
        this: void,
        ctx: HookContext,
        tx: OutgoingTransaction,
        uuid: string,
        err: SendError,
    ) => void;

    private constructor(state: State, stream: TransactionStream) {
        this.id = state.state.id;
        this._state = state;
        this._stream = stream;
    }

    /**
     * Opens a stream from a given directory.
     * 
     * ```lua
     * if not fs.isDir("/stream") then
     *     kstream.Stream.create("/stream", "https://krist.dev", "kpg2310002")
     * end
     * local stream = kstream.Stream.open("/stream")
     * ```
     * 
     * @param dir The directory that the stream is stored at.
     * @param revision An optional revision number. See {@link HookContext.onPrepare()}
     * for more info.
     * @returns The stream.
     */
    public static open(this: void, dir: string, revision?: number): Stream {
        expect(1, dir, "string");
        const state = State.open(dir, revision);
        const set = new TransactionSet(state.state.includeMined, state.state.address);
        const stream = new TransactionStream(
            state.state.endpoint,
            set,
            state.state.lastPoppedId,
        );
        return new Stream(state, stream);
    }

    /**
     * Creates a new stream at a given directory.
     * 
     * ```lua
     * if not fs.isDir("/stream") then
     *     kstream.Stream.create("/stream", "https://krist.dev", "kpg2310002")
     * end
     * ```
     * 
     * @param dir The directory to store the stream at.
     * @param endpoint The Krist node URL to use for this stream.
     * @param address The address to fetch transactions for, or all addresses if nil.
     * @param includeMined Whether to include mining reward transactions.
     */
    public static create(
        this: void,
        dir: string,
        endpoint: string,
        address?: string,
        includeMined?: boolean,
    ): void {
        expect(1, endpoint, "string");
        expect(2, dir, "string");
        expect(3, address, "string", "nil");
        expect(4, includeMined, "boolean", "nil");
        State.create(dir, endpoint, !!includeMined, address);
    }

    /**
     * Closes open Krist sockets to prevent them from lingering after the stream.
     *
     * You are encorauged to call this method when cleaning up a program, preferably
     * after catching any errors, and outside of any parallel API calls.
     */
    public close(): void {
        expect(1, this, "table");
        this._stream.closeSocket();
    }

    /** Receives transactions into the state inbox and calls the transaction hook. */
    private _inboxWorker() {
        while (true) {
            const held = this._fetch();
            assert(this._state.state.committed.inbox);
            const onTransaction = this.onTransaction;
            if (type(onTransaction) == "function") {
                const inner = new InnerHookContext(this._state, held);
                const inbox = assert(inner.uncommitted.inbox);
                const tx = parseApiTx(inbox);
                inner.uncommitted.inbox = undefined;
                this._runHook(onTransaction!, inner, [tx]);
                held.unlock();
            } else {
                held.unlock();
                expect.field(this, "onTransaction", "function");
                throw "unreachable";
            }
        }
    }

    /** Sends transactions in the state outbox and calls the appropriate hooks. */
    private _outboxWorker() {
        while (true) {
            const held = this._state.lock();
            const outbox = this._state.state.committed.outbox;
            if (outbox.length > 0) {
                const { id, transaction: tx } = outbox[0];
                const [ok, err] = this._send(held);
                if (ok) {
                    const onSendSuccess = this.onSendSuccess;
                    if (onSendSuccess != undefined) {
                        if (type(onSendSuccess) == "function") {
                            const inner = new InnerHookContext(this._state, held);
                            assert(table.remove(inner.uncommitted.outbox, 1));
                            this._runHook(onSendSuccess, inner, [tx, id]);
                            held.unlock();
                        } else {
                            held.unlock();
                            expect.field(this, "onSendSuccess", "function");
                            throw "unreachable";
                        }
                    } else {
                        assert(table.remove(outbox, 1));
                        this._state.commit();
                        held.unlock();
                    }
                } else {
                    const onSendFailure = this.onSendFailure;
                    if (onSendFailure != undefined) {
                        if (type(onSendFailure) == "function") {
                            const inner = new InnerHookContext(this._state, held);
                            assert(table.remove(inner.uncommitted.outbox, 1));
                            this._runHook(onSendFailure, inner, [tx, id, err!]);
                            held.unlock();
                        } else {
                            held.unlock();
                            expect.field(this, "onSendFailure", "function");
                            throw "unreachable";
                        }
                    } else {
                        assert(table.remove(outbox, 1));
                        this._state.commit();
                        held.unlock();
                    }
                }
            } else {
                held.unlock();
                os.pullEvent("kstream_mutex_unlocked");
                os.pullEvent("kstream_mutex_unlocked");
            }
        }
    }

    /** Runs the stream's background routines. */
    public run() {
        expect(1, this, "table");
        const held = this._routineMutex.lock();
        const [ok, err] = pcall(() => {
            parallel.waitForAll(
                () => this._stream.listen(),
                () => this._inboxWorker(),
                () => this._outboxWorker(),
            )
        });
        held.unlock();
        if (!ok) { throw err; }
    }

    /**
     * Checks if the node is up.
     * 
     * This method sends no new requests. Checks are made through the socket and return
     * false about 30 seconds after the node is down.
     * 
     * @returns Whether the node was up since the last check.
     */
    public isUp(): boolean {
        expect(1, this, "table");
        return this._stream.isUp();
    }

    /**
     * Fetches and writes a transaction into the inbox.
     * 
     * Commits the state with the new transaction on the inbox on success.
     * 
     * This method does not hold the state mutex while it waits for a transaction, but
     * acquires it to write the inbox. Then, the method returns the held mutex to the
     * caller so it can work on the transaction.
     */
    private _fetch(): HeldMutex {
        // Check if there is something in the inbox.
        const stateHeld1 = this._state.lock();
        const inbox = this._state.state.committed.inbox;
        if (inbox) { return stateHeld1; }
        stateHeld1.unlock();

        // Wait for a new transaction on the stream.
        const streamHeld = this._stream.mutex.lock();
        this._stream.wait();

        // Put it into the inbox.
        const stateHeld2 = this._state.lock();
        if (!this._state.state.committed.inbox) {
            const incoming = this._stream.pop();
            this._state.state.committed.inbox = incoming;
            this._state.state.lastPoppedId = incoming.id;
            this._state.commit();
        }
        streamHeld.unlock();
        return stateHeld2;
    }

    /** Returns whether an outbox entry was already sent. */
    private _wasSent(_: HeldMutex, outgoing: OutboxEntry): boolean {
        if (outgoing.status == OutboxStatus.PENDING) { return false; }
        if (outgoing.status == OutboxStatus.SENT) { return true; }
        const ref = outgoing.ref;
        const url = this._state.state.endpoint + "/search/extended?q=" + ref;
        const handle = assert(request({ method: "GET", url }));
        assert(handle.ok, handle.msg);
        const s = handle.h.readAll() || "";
        const obj = parseJson(s, url);
        const res = expectOk<ApiSearchResponse>(obj);
        return res.matches.transactions.metadata != 0;
    }

    /**
     * Fetches an account's balance.
     * 
     * This method returns nil if the account doesn't exist, the timeout value is
     * reached, or another error is encountered.
     * 
     * @param address The address to look up.
     * @param timeout A timeout to give up looking.
     * @returns The balance, or nil on failure.
     */
    public getBalance(address: string, timeout?: number): number | undefined {
        expect(1, this, "table");
        expect(1, address, "string");
        expect(2, timeout, "number", "nil");
        const deadline = timeout && os.clock() + timeout;
        const url = this._state.state.endpoint + "/addresses/" + address;
        const handle = request({ url, method: "GET" }, deadline);
        if (!handle) { return; }
        assert(handle.ok, handle.msg);
        const s = handle.h.readAll() || "";
        const obj: ApiAddressResponse | ApiError = parseJson(s, url);
        if (!obj.ok) { return; }
        return obj.address.balance;
    }

    /**
     * Runs a user hook with the given arguments under a transactional state context.
     * 
     * The given inner hook context will be committed after this function returns.
     * 
     * If the hook throws then we catch, abort the transaction, unlock the mutex, and
     * rethrow. The caller is not expected to catch these exceptions.
     * 
     * The hook may define further hooks to be invoked, like afterCommit, which will get
     * called before this method returns. Thrown exceptions will also unlock the mutex.
     */
    private _runHook<F extends (this: void, ctx: HookContext, ...args: any[]) => void>(
        fn: F,
        inner: InnerHookContext,
        args: Parameters<F> extends [infer _, ...infer R] ? R : never,
    ): void {
        const outer = new HookContext(inner);
        const [ok, err] = pcall(() => {
            fn(outer, ...args);
            expect.field(outer, "onPrepare", "function", "nil");
        });
        if (ok) {
            if (outer.onPrepare) {
                const revision = inner.prepare();
                const [ok, err] = pcall(outer.onPrepare, revision);
                if (!ok) {
                    inner.held.unlock();
                    throw err;
                }
            }
            inner.commit();
            if (outer.afterCommit) {
                const [ok, err] = pcall(outer.afterCommit);
                if (!ok) {
                    inner.held.unlock();
                    throw err;
                }
            }
        } else {
            inner.abort();
            inner.held.unlock();
            throw err;
        }
    }

    /**
     * Executes an anonymous hook.
     * 
     * This method yields to acquire the state mutex before running the hook. If the
     * Krist node is unresponsive, it may never run. A timeout is available to avoid
     * waiting forever.
     * 
     * ```lua
     * stream:begin(function(ctx)
     *     ctx:enqueueSend({
     *         to = "pg231@switchcraft.kst",
     *         amount = 20,
     *         meta = {
     *             message = "hello",
     *         },
     *     })
     * end)
     * ```
     * 
     * @param fn The hook function.
     * @param timeout A timeout to give up waiting for the state mutex.
     * @returns true if the hook ran, or false if it timed out waiting.
     */
    public begin(fn: (ctx: HookContext) => void, timeout?: number): boolean {
        expect(1, this, "table");
        expect(1, fn, "function");
        const held = this._state.tryLock(timeout);
        if (!held) { return false; }
        this._runHook(fn, new InnerHookContext(this._state, held), []);
        held.unlock();
        return true;
    }

    /**
     * Enqueues a transaction for sending.
     * 
     * This method is an alias for {@link begin()} with a hook that just enqueues the
     * transaction.
     * 
     * @param tx The transaction.
     * @param timeout A timeout to give up waiting for the state mutex.
     * @returns The local transaction tracker UUID, or nil on timeout.
     */
    public send(tx: OutgoingTransaction, timeout?: number): string | undefined {
        expect(1, this, "table");
        expect(1, tx, "table");
        expect(2, timeout, "number", "nil");
        let out;
        this.begin((ctx) => { out = ctx.enqueueSend(tx) }, timeout);
        return out;
    }

    /**
     * Tries to send the first transaction on the outbox.
     * 
     * Commits the state setting the appropriate transaction status.
     * 
     * @returns Whether the send succeeded.
     * @returns The API error on failure.
     * @throws If the outbox is empty.
     */
    private _send(held: HeldMutex): LuaMultiReturn<[boolean, ApiError | undefined]> {
        const outgoing = assert(this._state.state.committed.outbox[0]);
        if (this._wasSent(held, outgoing)) {
            outgoing.status = OutboxStatus.SENT;
            this._state.commit();
            return $multi(true, undefined);
        }

        const meta = [];
        for (const [k, v] of outgoing.transaction.meta) { meta.push(k + "=" + v); }
        meta.push("ref=" + outgoing.ref);
        const metaStr = table.concat(meta, ";");

        const url = this._state.state.endpoint + "/transactions/";
        const body = textutils.serializeJSON({
            privatekey: outgoing.transaction.privateKey,
            to: outgoing.transaction.to,
            amount: outgoing.transaction.amount,
            metadata: metaStr,
        });

        // Switch status to unknown since we're about to send it.
        outgoing.status = OutboxStatus.UNKNOWN;
        this._state.commit();

        // Keep trying until we time out or succeed.
        while (true) {
            // Submit.
            const [response, _, error] = http.post({
                url,
                body,
                method: "POST",
                timeout: MAX_TX_TIMEOUT,
                headers: { "Content-Type": "application/json" },
            });

            if (response) {
                const s = response.readAll() || "";
                const obj = parseJson(s, url);
                if (obj.ok) {
                    // We're done.
                    outgoing.status = OutboxStatus.SENT;
                    this._state.commit();
                    return $multi(true, undefined);
                } else {
                    // We have a certified failure.
                    outgoing.status = OutboxStatus.PENDING;
                    this._state.commit();
                    return $multi(false, obj);
                }
            } else if (error) {
                const s = error.readAll() || "";
                const obj = parseJson(s, url);
                if (obj.ok) {
                    // ???
                    throw "server returned an inconsistent error value";
                } else {
                    // We have a certified failure.
                    outgoing.status = OutboxStatus.PENDING;
                    this._state.commit();
                    return $multi(false, obj);
                }
            } else {
                // Network error.
                if (this._wasSent(held, outgoing)) {
                    outgoing.status = OutboxStatus.SENT;
                    this._state.commit();
                    return $multi(true, undefined);
                }
            }
        }
    }
}

/** An error from an outgoing transaction failure. */
export type SendError = {
    /** An error code. */
    error: string,

    /** A human-readable message explaining the error. */
    message?: string,
}
