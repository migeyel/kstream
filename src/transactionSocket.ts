import { request } from "./http";
import { expectOk, ApiTransaction, parseJson } from "./util";
import { reseed } from "./uuid";

/** How long to wait before timing out when establishing a socket. */
const SOCKET_TIMEOUT = 10;

/** How long to wait before throwing away an unresponsive socket. */
const PING_TIMEOUT = 30;

type ApiSocketResponse = {
    ok: true,

    /** The socket URL to connect to. */
    url: string,
};

/** A class for fetching *all* incoming transactions from a Krist socket. */
export class TransactionSocket {
    /** The Krist endpoint. */
    private _endpoint: string;

    /** The current socket URL, if any. */
    private _url?: string;

    /** The current socket, if any. */
    private _socket?: WebSocket;

    /** The last UTC epoch a socket message was received. */
    private _lastPing = 0;

    /** The transaction callback. */
    private _transactionCb: (tx: ApiTransaction) => void;

    /** The socket status callback. */
    private _statusCb: (isUp: boolean) => void;

    public constructor(
        endpoint: string,
        transactionCb: (tx: ApiTransaction) => void,
        statusCb: (isUp: boolean) => void,
    ) {
        this._endpoint = endpoint;
        this._transactionCb = transactionCb;
        this._statusCb = statusCb;
    }

    /** Closes the existing socket, if any. */
    public close() {
        if (this._socket) {
            this._socket.close();
            this._url = undefined;
            this._socket = undefined;
        }
    }

    /** Reopens a socket. */
    private _reopen() {
        this.close();
        const url = this._endpoint + "/ws/start";
        while (true) {
            const handle = request({ method: "POST", url, body: "{}" })!;
            assert(handle.ok, handle.msg);
            const obj = parseJson(handle.h.readAll() || "", url);
            const response = expectOk<ApiSocketResponse>(obj);
            const [socket] = http.websocket({
                url: response.url,
                timeout: SOCKET_TIMEOUT,
            });
            if (socket) {
                this._url = response.url;
                reseed(response.url);
                this._socket = socket;
                this._lastPing = os.epoch("utc");
                this._socket.send(textutils.serializeJSON({
                    id: 0,
                    type: "subscribe",
                    event: "transactions",
                }));
                return;
            }
        }
    }

    private _onMessage(message: string) {
        const obj = assert(textutils.unserializeJSON(message));
        if (obj.type != "event") { return; }
        if (obj.event != "transaction") { return; }
        const tx = <ApiTransaction>obj.transaction;
        this._transactionCb(tx);
    }

    /** Opens the socket, listens for transactions, and performs housekeeping. */
    public listen() {
        if (!this._socket) { this._reopen(); }
        while (true) {
            const now = os.epoch("utc");
            const timeout = math.max(0, this._lastPing + 1000 * PING_TIMEOUT - now);
            const keepaliveTimer = os.startTimer(timeout / 1000);
            while (true) {
                const ev = os.pullEvent();
                if (ev[0] == "timer" && ev[1] == keepaliveTimer) {
                    this._statusCb(false);
                    this._reopen();
                    break;
                } else if (ev[0] == "websocket_message" && ev[1] == this._url) {
                    this._statusCb(true);
                    this._lastPing = os.epoch("utc");
                    this._onMessage(ev[2]);
                    os.cancelTimer(keepaliveTimer);
                    break;
                } else if (ev[1] == "websocket_closed" && ev[1] == this._url) {
                    this._statusCb(false);
                    this._reopen();
                    os.cancelTimer(keepaliveTimer);
                    break;
                }
            }
        }
    }
}
