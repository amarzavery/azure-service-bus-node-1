export function tickPromise(ticks: number = 100): Promise<void> {
    if (ticks <= 1) {
        return Promise.resolve();
    }
    return Promise.resolve().then(() => tickPromise(ticks - 1));
}
