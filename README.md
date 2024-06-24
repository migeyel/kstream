# Kstream

Kstream is callback-based node-agnostic Krist API library for CC:Tweaked.

It is not as feature-rich in its access to the API: it only supports receiving
transactions, sending transfers, and querying balances. Instead, it focuses on enabling
reliable operations around this restricted feature set.

Kstream ensures that every transaction made after its state file was created is fed to
the user-defined callback (the `onTransaction` hook). If a transaction was made while
the computer was turned off, Kstream backtracks through transaction history and
continues from where it left off. If the hook errors or the computer shuts down inside
it, the same transaction will be fed into the hook the next time Kstream starts up.

Outgoing transactions are committed to disk before being submitted to the Krist node,
and are reissued every time the computer restarts or if the network times out. A custom
meta `ref` field contains an UUID to prevent the same transaction being issued twice.
Users can define custom transaction userdata to inform the failure hook about how to
undo a failed transaction's effects to make the user whole.

Advanced users can set the `onPrepare` hook callback to atomically commit an external
state together with a hook's completion. This enables transactions to get issued or
acknowleged if and only if an external state has been changed alongside it.

CommonMeta fields are automatically parsed and encoded using a sensible interpretation
of how it's currently used and presented as a key-value table to the user. Transactions
also have a computed timestamp to let the user handle old transactions during
backtracking.

## Example

The code below is an annotated example usage of Kstream. The program refunds all
transactions, and calls `turtle.drop()` whenever it does so successfully.

Note that this program can sucessfully refund and drop nothing if it shuts down between
these two steps. At the same time, it will never drop twice for a single refund. A more
complete solution would require writing an external state to commit the (refund, drop)
action atomically and synchronize it using `onPrepare`.

```lua
local kstream = require "kstream"

local pkey = "changeme"
local address = kstream.makev2address(pkey)
print(address)

if not fs.isDir("/stream") then
    -- Streams can take any Krist-compatible API endpoint, and can either report all
    -- transactions or a subset that maches an address. It can also include mining
    -- rewards.
    kstream.Stream.create("/stream", "https://krist.dev", address)
end

local stream = kstream.Stream.open("/stream")

-- Runs on every transaction matched by the stream.
function stream.onTransaction(context, transaction)
    -- Code here (before commit) runs at least once per transaction.
    -- If they fail they run again with the same transaction until they succeed.

    if transaction.type ~= "transfer" then return end
    if transaction.to ~= address then return end
    if transaction.from == address then return end

    print("Received", transaction.value, "Krist from", transaction.from)
    print("Sending back")

    local meta = {}
    local userdata = { cookies = 1 }
    local refund = kstream.makeRefund(pkey, address, transaction, meta, userdata)
    if refund then
        -- enqueueSend only sends out transactions when the hook commits.
        -- Since hooks only run again if they didn't commit, this transaction will get
        -- sent exactly once.
        context:enqueueSend(refund)
    end
end

-- Runs on every transaction sent by us that succeeded.
function stream.onSendSuccess(context, transaction, uuid)
    print("Sent", transaction.amount, "KST to", transaction.to)

    function context.afterCommit()
        -- Code here (after commit) runs at most once per transaction.
        -- You can use it to perform side-effects that aren't sending transactions
        -- without risking running twice. Other hooks cannot run concurrently to this
        -- function.

        local userdata = transaction.ud
        if userdata and userdata.cookies == 1 then
            print("Dispensing cookie")
            turtle.select(1)
            turtle.drop(1)
        end
    end
end

-- All three main hooks are required to be defined. They will error otherwise.

-- Runs on every transaction sent by us that failed.
function stream.onSendFailure(context, transaction, uuid, error)
    print("Failed to send", transaction.amount, "KST to", transaction.to)
end

-- Run and call close on error so the websocket doesn't linger.
local ok, err = pcall(function() stream:run() end)
stream:close()
assert(ok, err)
```
