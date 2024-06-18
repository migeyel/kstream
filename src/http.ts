import { timeoutOrDeadline } from "./util";
import { uuid4 } from "./uuid";

/** An HTTP response, either success or failure. */
export class FusedHttpResponse {
    /** Whether the response was ok or not. */
    public readonly ok: boolean;

    /** The HTTP status message. */
    public readonly msg: string;

    /** The response handle. */
    public readonly h: HTTPResponse;

    public constructor(h: HTTPResponse) {
        const [code, msg] = h.getResponseCode();
        this.ok = code >= 200 && code < 300;
        this.msg = msg;
        this.h = h;
    }
}

/**
 * Keeps attempting to perform an HTTP request until it either succeeds or a deadline is
 * hit.
 * @param req The request options. The timeout attribute is overwritten.
 * @param deadline The deadline, hit when os.clock() is greater than it.
 * @returns The response handle, or nil on timeout.
 */
export function request(
    req: RequestOptions & { method: string },
    deadline?: number,
): FusedHttpResponse | undefined {
    deadline = deadline || math.huge;
    let exp = 3;
    while (true) {
        const timeout = timeoutOrDeadline(math.random(5, 15) / 10 * exp, deadline);
        if (timeout == 0) { return; }
        const t0 = os.clock();
        const url = req.url + "#" + uuid4();
        const opts = { body: "", ...req, url, timeout: math.min(60, timeout) };
        const [h1, _, h2] = http.post(opts);
        if (h1) {
            return new FusedHttpResponse(h1);
        } else if (h2) {
            return new FusedHttpResponse(h2);
        }
        sleep(timeout - os.clock() + t0);
        exp = math.min(1.5 * exp, 60);
    }
}
