import * as expect from "cc/expect";
import sha256 from "./sha256";
import { hex } from "./util";
import { Transfer } from "./transaction";
import { OutgoingTransaction } from "./state";

export { Stream, Result } from "./stream";

function addrByte(byte: number) {
    const n = 48 + math.floor(byte / 7);
    return string.char(n + 39 > 122 ? 101 : n > 57 ? n + 39 : n);
}

/**
 * Derives a v2 address from a private key.
 * @param key The private key to derive.
 * @param prefix An optional address prefix, defaults to `"k"`.
 * @returns The address.
 */
export function makev2address(this: void, key: string, prefix = "k"): string {
    expect(1, key, "string");
    expect(2, prefix, "string");

    let stick = sha256(hex(sha256(key)));
    const protein = new LuaMap<number, number>();
    for (const i of $range(0, 8)) {
        protein.set(i, string.byte(stick));
        stick = sha256(hex(sha256(hex(stick))));
    }

    let n = 1;
    const out = [prefix];
    while (n <= 8) {
        const link = string.byte(stick, n) % 9;
        const byte = protein.get(link);
        if (byte) {
            out.push(addrByte(byte));
            protein.delete(link);
            n++;
        } else {
            stick = sha256(hex(stick));
        }
    }

    for (const i of $range(0, 8)) {
        const byte = protein.get(i);
        if (byte) {
            out.push(addrByte(byte));
            return table.concat(out);
        }
    }

    throw "unreachable";
}

/**
 * Makes a refund for a transfer.
 * @param privateKey The private key of the receiver.
 * @param address The address of the matching private key, for performance.
 * @param transfer The incoming transfer.
 * @param meta Extra metadata to include.
 * @param ud Extra optional user data.
 * @returns The refund pending transfer to set, or nil when unsafe to do so.
 */
export function makeRefundFor(
    this: void,
    privateKey: string,
    address: string,
    transfer: Transfer,
    meta = new LuaMap<string, string>(),
    ud?: any,
): OutgoingTransaction | undefined {
    expect(1, privateKey, "string");
    expect(2, address, "string");
    expect(3, transfer, "table");
    expect(4, meta, "table");

    expect.field(transfer, "type", "string");
    expect.field(transfer, "from", "string");
    expect.field(transfer, "to", "string");
    expect.field(transfer, "kv", "table");
    expect.field(transfer, "value", "number");

    if (transfer.type != "transfer") { return; }
    if (transfer.from == address) { return; }
    if (transfer.to != address) { return; }
    if (transfer.kv.get("return") == "false") { return; }
    if (transfer.kv.has("error")) { return; }

    return {
        amount: transfer.value,
        privateKey,
        to: transfer.kv.get("return") || transfer.from,
        meta: { ...meta, return: "false" },
        ud,
    };
}
