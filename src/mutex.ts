export class HeldMutex {
    private mutex: Mutex;

    public constructor(mutex: Mutex) {
        this.mutex = mutex;
    }

    public unlock(): void {
        this.mutex.unlock(this);
    }
}

/** A mutex. */
export class Mutex {
    private held?: HeldMutex;

    public lock(): HeldMutex {
        while (this.held) { os.pullEvent("kstream_mutex_unlocked"); }
        this.held = new HeldMutex(this);
        return this.held;
    }

    public tryLock(deadline?: number): HeldMutex | undefined {
        if (!deadline) { return this.lock(); }

        const timer = os.startTimer(deadline - os.clock());
        while (this.held) {
            const [event, p1] = os.pullEvent();
            if (event == "timer" && p1 == timer) { return; }
        }

        os.cancelTimer(timer);
        this.held = new HeldMutex(this);
        return this.held;
    }

    public isHeld(held: HeldMutex): boolean {
        return this.held == held;
    }

    public unlock(held: HeldMutex): void {
        assert(this.held == held);
        this.held = undefined;
        os.queueEvent("kstream_mutex_unlocked");
    }
}
