import * as expect from "cc/expect";
import { parseApiTx, Transaction } from "./transaction";
import { copy } from "./util";
import { uuid4 } from "./uuid";
import { Boxes, OutboxStatus, OutgoingTransaction, State } from "./state";
import { HeldMutex } from "./mutex";

export enum BoxViewStatus {
    /** The box view is accepting changes and will commit later. */
    UNCOMMITTED = "uncommitted",

    /** The box view is prepared for committing. */
    PREPARED = "prepared",

    /** The box view has been committed. */
    COMMITTED = "committed",

    /** The box view commit has been aborted. */
    ABORTED = "aborted",
}

/**
 * An exclusive view into the stream's transaction inbox and outbox state.
 *
 * This object is used to atomically manipulate the transaction inbox and outbox slots.
 * The state will not get saved to disk until {@link commit()} is called. Other stream
 * operations that manipulate these slots will also error until this view is closed.
 */
export class BoxView {
    /** The global state. */
    private _state: State;

    /** The commit status of this view. */
    private _status = BoxViewStatus.UNCOMMITTED;

    /** A copy for uncommitted modifications to the boxes. */
    private _uncommitted: Boxes;

    /** The held mutex for the state. */
    private _heldMutex: HeldMutex;

    public constructor(state: State, heldMutex: HeldMutex) {
        assert(!state.state.prepared); // Should be handled by caller.
        assert(state.mutex.isHeld(heldMutex));
        this._state = state;
        this._uncommitted = copy(state.state.committed);
        this._uncommitted.revision++;
        this._heldMutex = heldMutex;
    }

    private _checkStatus(s: string) {
        if (this._status != BoxViewStatus.UNCOMMITTED) {
            error(this._status + " box views can't be " + s);
        }
    }

    /**
     * Returns the transaction at the inbox, if any.
     * @returns The inbox transaction, if any.
     */
    public peekInbox(): Transaction | undefined {
        expect(1, this, "table");
        this._checkStatus("read");
        const out = this._uncommitted.inbox;
        if (!out) { return; }
        return parseApiTx(out);
    }

    /**
     * Removes the transaction at the inbox and returns it.
     * @returns The former inbox transaction, if any.
     */
    public popInbox(): Transaction | undefined {
        expect(1, this, "table");
        this._checkStatus("read nor written");
        const out = this._uncommitted.inbox;
        if (!out) { return; }
        this._uncommitted.inbox = undefined;
        return parseApiTx(out);
    }

    /**
     * Returns whether the outbox transaction is in a known state.
     * @returns Whether the outbox transaction is in a known state.
     */
    public isOutboxKnown(): boolean {
        const outbox = this._uncommitted.outbox;
        if (!outbox) { return true; }
        return outbox.status != OutboxStatus.UNKNOWN;
    }

    /**
     * Returns the outbox transaction.
     * @returns The transaction, or nil if there is none.
     * @throws If the outbox transaction is in an unknown state.
     */
    public getOutbox(): OutgoingTransaction | undefined {
        expect(1, this, "table");
        this._checkStatus("read");
        const outbox = this._uncommitted.outbox;
        if (!outbox) { return; }
        if (outbox.status == OutboxStatus.UNKNOWN) {
            throw "attempt to get unknown outbox";
        }
        return copy(outbox.transaction);
    }

    /**
     * Sets or clears an outbox transaction.
     * @param tx The transaction to set, or nil to clear the outbox.
     */
    public setOutbox(tx?: OutgoingTransaction) {
        expect(1, this, "table");
        expect(1, tx, "table", "nil");
        this._checkStatus("written");
        if (tx == undefined) {
            this._uncommitted.outbox = undefined;
        } else {
            expect.field(tx, "amount", "number");
            expect.field(tx, "to", "string");
            expect.field(tx, "privateKey", "string");
            expect.field(tx, "meta", "table");
            this._uncommitted.outbox = {
                status: OutboxStatus.PENDING,
                ref: uuid4(),
                transaction: copy(tx),
            };
        }
    }

    /**
     * Prepares the state for a commit. Used for coordinating an atomic commit with an
     * external store.
     *
     * To do that:
     * 1. Open the box view.
     * 2. Make changes to the inbox, outbox, and the external store.
     * 3. Call {@link prepare}.
     * 4. Write the returned value into the external store.
     * 5. Commit or abort the external store.
     * 6. Call {@link commit}, or {@link abort}, depending on the previous step.
     *
     * After a crash, call {@link Stream.open} with the value written in step 4 to
     * recover the proper stream state. Since {@link Stream.open} allows to choose
     * between the prepared and committed states, step 5 is capable of committing both
     * the external store and the Kstream state atomically.
     *
     * @returns A *revision* value that can be input into {@link Stream.open} after
     * a system crash to either commit or abort this view's changes.
     * @throws If the view has already been prepared, committed, or aborted.
     */
    public prepare(): number {
        expect(1, this, "table");
        this._checkStatus("prepared");
        this._status = BoxViewStatus.PREPARED;
        this._state.state.prepared = this._uncommitted;
        this._state.commit();
        return this._uncommitted.revision;
    }

    /**
     * Commits the box view state to disk.
     * @throws If the view has already been committed or aborted.
     */
    public commit() {
        expect(1, this, "table");
        const status = this._status;
        if (status != BoxViewStatus.UNCOMMITTED && status != BoxViewStatus.PREPARED) {
            error(this._status + " box views can't be committed");
        }
        this._status = BoxViewStatus.COMMITTED;
        this._state.state.committed = this._uncommitted;
        this._state.state.prepared = undefined;
        this._state.commit();
        this._state.mutex.unlock(this._heldMutex);
    }

    /**
     * Discards all changes performed and closes this view.
     * @throws If the view has already been committed or aborted.
     */
    public abort() {
        expect(1, this, "table");
        const status = this._status;
        if (status != BoxViewStatus.UNCOMMITTED && status != BoxViewStatus.PREPARED) {
            error(this._status + " box views can't be aborted");
        }
        this._state.state.prepared = undefined;
        this._state.commit();
        this._state.mutex.unlock(this._heldMutex);
        this._status = BoxViewStatus.ABORTED;
    }
}
