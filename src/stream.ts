import * as expect from "cc/expect";
import { OutboxStatus, State } from "./state";
import { TransactionSet } from "./transactionSet";
import { TransactionStream } from "./transactionStream";
import { BoxView } from "./boxView";
import { request } from "./http";
import { ApiAddressResponse, ApiError, ApiSearchResponse, expectOk, parseJson, timeoutOrDeadline } from "./util";
import { HeldMutex } from "./mutex";

const MAX_TX_TIMEOUT = 10;

/** The result of a networked operation. */
export enum Result {
    /** The operation succeeded. */
    ok = "ok",

    /** The operation failed. */
    error = "error",

    /** We timed out waiting for a response. It may have succeeded or failed. */
    timeout = "timeout",
}

/** A disk-backed persistent stream of transactions. */
export class Stream {
    /** The main state. */
    private _state: State;

    /** The underlying in-memory stream. */
    private _stream: TransactionStream;

    // private _incomingHandlers = new Lu

    private constructor(state: State, stream: TransactionStream) {
        this._state = state;
        this._stream = stream;
    }

    /**
     * Opens a stream from a given directory.
     * @param dir The directory that the state is stored at.
     * @param revision If {@link BoxView.prepare} was called, followed by a sytem crash,
     * you can use its return value here to commit it after the fact, or some other
     * value, including nil, to abort it.
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
     * @param dir The directory to store the stream at.
     * @param endpoint The Krist node URL to use for this stream.
     * @param address The address to fetch transactions for, or all of them if nil.
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
     * Users are encorauged to call this method when cleaning up a program, preferably
     * after catching any errors, and outside of any parallel API calls.
     */
    public close(): void {
        expect(1, this, "table");
        this._stream.closeSocket();
    }

    /**
     * Listens for new transactions through the socket. Must run in parallel to other
     * routines.
     */
    public listen() {
        expect(1, this, "table");
        return this._stream.listen();
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
     * Returns a view into the inbox and outbox transaction slots.
     * 
     * This view must either be committed to disk or aborted before using any other
     * method that reads or writes to these slots.
     * 
     * If another thread has an open view, this thread will yield until that is no
     * longer the case, and then return.
     * 
     * @returns The box view.
     */
    public getBoxView(): BoxView {
        expect(1, this, "table");
        return new BoxView(this._state, this._state.mutex.lock());
    }

    /**
     * Fetches the next transaction from the stream and puts it in the inbox.
     *
     * If the inbox is already full, this method does nothing.
     */
    public fetch(): void {
        expect(1, this, "table");

        // Check if there is something in the inbox.
        const stateHeld1 = this._state.mutex.lock();
        const inbox = this._state.state.committed.inbox;
        stateHeld1.unlock();
        if (inbox) { return; }

        // Wait for a new transaction on the stream.
        const streamHeld = this._stream.mutex.lock();
        this._stream.wait();

        // Put it into the inbox.
        const stateHeld2 = this._state.mutex.lock();
        if (!this._state.state.committed.inbox) {
            const incoming = this._stream.pop();
            this._state.state.committed.inbox = incoming;
            this._state.state.lastPoppedId = incoming.id;
            this._state.commit();
        }
        streamHeld.unlock();
        stateHeld2.unlock();
    }

    private _resolveOutbox(_: HeldMutex, deadline?: number): boolean {
        const outgoing = this._state.state.committed.outbox;
        if (!outgoing) {
            // There's no transaction to resolve.
            return true;
        }

        if (outgoing.status == OutboxStatus.UNKNOWN) {
            // Check if the transaction wasn't already sent.
            const ref = outgoing.ref;
            const url = this._state.state.endpoint + "/search/extended?q=" + ref;
            const handle = request({ method: "GET", url }, deadline);
            if (!handle) {
                // The request timed out.
                return false;
            }
            assert(handle.ok, handle.msg);
            const s = handle.h.readAll() || "";
            const obj = parseJson(s, url);
            const res = expectOk<ApiSearchResponse>(obj);
            if (res.matches.transactions.metadata == 0) {
                // The transaction hasn't been sent.
                outgoing.status = OutboxStatus.PENDING;
                this._state.commit();
            } else {
                // The transaction has already been sent.
                this._state.state.committed.outbox = undefined;
                this._state.commit();
            }
        }

        return true;
    }

    /**
     * Tries to resolve whether the transaction on the outbox has been sent.
     *
     * If this call returns true, then {@link BoxView.getOutbox} will not return
     * "unknown" unless {@link send} gets called and fails.
     *
     * @param timeout A timeout to give up on deciding.
     * @returns Whether the resolution was successful.
     */
    public resolveOutbox(timeout?: number): boolean {
        expect(1, this, "table");
        expect(1, timeout, "number", "nil");
        const deadline = timeout && os.clock() + timeout;
        const held = this._state.mutex.tryLock(deadline);
        if (!held) { return false; }
        const out = this._resolveOutbox(held, deadline);
        held.unlock();
        return out;
    }

    /**
     * Fetches an account's balance.
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
     * Tries to send the transaction on the outbox.
     *
     * If the outbox is already empty, this method does nothing.
     *
     * @param timeout A timeout to give up on sending.
     * @returns The result of the operation.
     */
    public send(timeout?: number): Result {
        expect(1, this, "table");
        expect(1, timeout, "number", "nil");

        const deadline = timeout && os.clock() + timeout;
        const held = this._state.mutex.tryLock(deadline);
        if (!held) { return Result.timeout; }

        const outgoing = this._state.state.committed.outbox;
        if (!outgoing) {
            // There's no transaction to send.
            held.unlock();
            return Result.ok;
        }

        if (!this._resolveOutbox(held, deadline)) {
            // The transaction status is unknown and we failed to resolve it.
            held.unlock();
            return Result.timeout;
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

        // Keep trying until we time out or succeed.
        while (true) {
            // Switch status to unknown since we're about to send it.
            outgoing.status = OutboxStatus.UNKNOWN;
            this._state.commit();
            
            // Submit.
            const [response, _, error] = http.post({
                url,
                body,
                method: "POST",
                timeout: math.min(60, timeoutOrDeadline(MAX_TX_TIMEOUT, deadline)),
                headers: { "Content-Type": "application/json" },
            });

            if (response) {
                const s = response.readAll() || "";
                const obj = parseJson(s, url);
                if (obj.ok) {
                    // We're done.
                    this._state.state.committed.outbox = undefined;
                    this._state.commit();
                    held.unlock();
                    return Result.ok;
                } else {
                    // We have a certified failure.
                    outgoing.status = OutboxStatus.PENDING;
                    this._state.commit();
                    held.unlock();
                    return Result.error;
                }
            } else if (error) {
                const s = error.readAll() || "";
                const obj = parseJson(s, url);
                if (obj.ok) {
                    // ???
                    held.unlock();
                    throw "server returned an inconsistent error value";
                } else {
                    // We have a certified failure.
                    outgoing.status = OutboxStatus.PENDING;
                    this._state.commit();
                    held.unlock();
                    return Result.error;
                }
            } else {
                // Network error, resolve and try again.
                if (!this._resolveOutbox(held, deadline)) {
                    // Ran out of time.
                    held.unlock();
                    return Result.timeout;
                }

                if (!this._state.state.committed.outbox) {
                    // Was already sent.
                    held.unlock();
                    return Result.ok;
                }

                // Resolved into pending, let the loop iterate again.
            }
        }
    }
}
