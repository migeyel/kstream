import { copy } from "./util";
import { uuid4 } from "./uuid";
import { Boxes, OutboxStatus, OutgoingTransaction, State } from "./state";
import { HeldMutex } from "./mutex";

export enum HookContextStatus {
    /** The box view is accepting changes and will commit later. */
    UNCOMMITTED = "uncommitted",

    /** The box view is prepared for committing. */
    PREPARED = "prepared",

    /** The box view has been committed. */
    COMMITTED = "committed",

    /** The box view commit has been aborted. */
    ABORTED = "aborted",
}

export class InnerHookContext {
    /** The global state. */
    public state: State;

    /** The commit status of this view. */
    public status = HookContextStatus.UNCOMMITTED;

    /** A copy for uncommitted modifications to the boxes. */
    public uncommitted: Boxes;

    /** The held mutex for the state. */
    public held: HeldMutex;

    public constructor(state: State, heldMutex: HeldMutex) {
        this.state = state;
        this.uncommitted = copy(state.state.committed);
        this.uncommitted.revision++;
        this.held = heldMutex;
    }

    public checkStatus(s: string) {
        if (this.status != HookContextStatus.UNCOMMITTED) {
            error(this.status + " box views can't be " + s);
        }
    }

    /** Prepares the state for a commit. */
    public prepare(): number {
        this.checkStatus("prepared");
        this.status = HookContextStatus.PREPARED;
        this.state.state.prepared = this.uncommitted;
        this.state.commit();
        return this.uncommitted.revision;
    }

    /** Commits the state to disk. */
    public commit() {
        const status = this.status;
        if (status != HookContextStatus.UNCOMMITTED && status != HookContextStatus.PREPARED) {
            error(this.status + " box views can't be committed");
        }
        this.status = HookContextStatus.COMMITTED;
        this.state.state.committed = this.uncommitted;
        this.state.state.prepared = undefined;
        this.state.commit();
    }

    /** Discards all changes performed and closes this view. */
    public abort() {
        const status = this.status;
        if (status != HookContextStatus.UNCOMMITTED && status != HookContextStatus.PREPARED) {
            error(this.status + " box views can't be aborted");
        }
        this.state.state.prepared = undefined;
        this.state.commit();
        this.status = HookContextStatus.ABORTED;
    }
}

/** Methods and fields usable by a hook to manipulate stream state. */
export class HookContext {
    private _inner: InnerHookContext;

    /**
     * Sets a callback to run after the hook prepares to commit, but before it commits.
     * 
     * Together with Stream.open(), this can be used to atomically commit a hook
     * together with an external state. Between a hook prepare and a hook commit, both
     * revision values (the previous and the current) will open into their appropriate
     * states.
     * 
     * If onPrepare errors, the entire stream is rendered unusable and must be
     * reopened. Thus, it is recommended to not catch any errors thrown by onPrepare
     * and instead restart the entire program to reload the correct revision from the
     * external state.
     * 
     * ```lua
     * local myState = readMyState()
     * myState.credits = myState.credits or 5
     * 
     * -- myState.revision contains the revision sent to us on onPrepare, or nil if
     * -- onPrepare wasn't called yet.
     * local revision = myState.revision
     * local stream = kstream.Stream.open("/stream", revision)
     * 
     * parallel.waitForAll(
     *     function() stream:run() end,
     *     function()
     *         while myState.credits > 0 do
     *             local txid = nil
     *             stream:begin(function(ctx)
     *                 myState.credits = myState.credits - 1
     *             
     *                 -- enqueueSend puts a transaction out, but we want that to happen
     *                 -- together with our credit deduction, so the receiver doesn't
     *                 -- get free Krist.
     *                 txid = ctx:enqueueSend({
     *                     to = "kpg2310002",
     *                     amount = 1,
     *                     meta = {
     *                         message = "Here are your credits!",
     *                     },
     *                 })
     *             
     *                 -- This is where onPrepare comes in.
     *                 -- - If we call open() with the previous revision, it will load
     *                 --   the previous state (no enqueued transaction).
     *                 -- - If we call open() with the new revision, it will load the
     *                 --   new state (enqueued transaction).
     *                 -- Since we write both the deduction and the new revision in a
     *                 -- single call, the enqueued transaction will be present iff the
     *                 -- deduction happened.
     *                 function ctx.onPrepare(revision)
     *                     myState.revision = revision
     *                     writeMyState(myState)
     *                 end
     *             end)
     *         end
     *     end
     * )
     * 
     * stream:close()
     * ```
     */
    public onPrepare?: (this: void, revision: number) => void;

    /**
     * Sets a callback to run after the hook commits.
     * 
     * If afterCommit errors, the error gets bubbled up. However, after a catch or a
     * restart, the stream will not re-run any hooks and instead will carry on to the
     * next hook as if afterCommit never errored.
     */
    public afterCommit?: (this: void) => void;

    public constructor(inner: InnerHookContext) {
        this._inner = inner;
    }

    /**
     * Enqueues a transaction to be sent after the hook commits.
     * 
     * If the hook errors, the enqueued transaction is dropped and the hook will
     * eventually re-run. As a result, it is safe to enqueue transactions in the main
     * hook body even if they may run several times.
     * 
     * ```lua
     * local stream = kstream.Stream.open("/stream")
     * local pkey = "changeme"
     * local address = kstream.makev2address(pkey)
     * 
     * -- Watch for transactions and always refund them.
     * function stream.onTransaction(ctx, tx)
     *     local refund = kstream.makeRefund(pkey, address, tx)
     *     if refund then ctx:enqueueSend(refund) end
     * end
     * 
     * stream:run()
     * ```
     * 
     * @param tx The transaction.
     * @returns A UUID for tracking the queued transaction with events.
     */
    public enqueueSend(tx: OutgoingTransaction): string {
        this._inner.checkStatus("written");
        const id = uuid4();
        this._inner.uncommitted.outbox.push({
            id,
            status: OutboxStatus.PENDING,
            ref: uuid4(),
            transaction: copy(tx),
        });
        return id;
    }
}
