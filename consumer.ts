import * as MessageQueue from 'svmq';
import {ChildProcessWithoutNullStreams, spawn} from 'child_process'

class Consumer {
    queue: any;
    goproc: ChildProcessWithoutNullStreams;
    interval: NodeJS.Timeout;

    constructor() {
        this.queue = new MessageQueue(12349)

        this.goproc = spawn("./bin/goo", ['abc'])
        this.goproc.stdout.on("data", (data) => {console.log("goproc stdout", data.toString())});
        this.goproc.on("error", err => {console.log("goproc err", err)})

        setTimeout(this.close.bind(this), 10e3);

        // this.echo()
    }

    async start() {
        this.queue.on('data', this.onData.bind(this))
        this.queue.on('error', this.onError.bind(this))
    }

    onData(data: Buffer) {
        console.log("node", data.toString())
    }

    onError(err) {
        console.log(err.message)
    }

    echo() {
        this.send("abc").then(() => {setTimeout(this.echo.bind(this), 1e3)}, console.error)
    }
    async send(str: string) {
        return new Promise<void>((res, rej) => {
            this.queue.push(Buffer.from(str), err => err ? rej(err) : res())
        })
    }

    async close() {
        clearInterval(this.interval);

        return new Promise<void>((res, rej) => {
            this.queue.close((err, closed) => err ? rej(err) : res(closed))
        })
    }
}

new Consumer().start().catch(console.error);

export default Consumer;
