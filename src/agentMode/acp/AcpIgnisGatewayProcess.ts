import { logError, logInfo, logWarn } from "@/logger";
import { sanitizeAcpStdout } from "./AcpProcessManager";

const CHANNEL = "plugin:ignis-agent-gateway";
const SHUTDOWN_GRACE_MS = 3_000;

interface IgnisChannelApi {
  subscribe(type: string, handler: (msg: GatewayMessage) => void): () => void;
  send(type: string, payload: Record<string, unknown>): void;
}

interface IgnisWsApi {
  channel(name: string): IgnisChannelApi;
  isOpen(): boolean;
}

interface GatewayMessage {
  channel?: string;
  type?: string;
  payload?: GatewayPayload;
}

interface GatewayPayload {
  channelId?: string;
  line?: string;
  code?: number | null;
  signal?: string | null;
  error?: string;
  pid?: number;
  backend?: string;
}

export interface IgnisGatewayProcessOptions {
  backend: string;
  vaultId: string;
  logTag?: string;
}

let channelCounter = 0;

export class AcpIgnisGatewayProcess {
  private readonly opts: IgnisGatewayProcessOptions;
  private readonly tag: string;
  private readonly channelId: string;
  private channel: IgnisChannelApi | null = null;
  private stdinBuffer = "";
  private pendingStdin: string[] = [];
  private opened = false;
  private exited = false;
  private exitCode: number | null = null;
  private exitSignal: string | null = null;
  private exitListeners = new Set<(code: number | null, signal: string | null) => void>();
  private stdoutController: ReadableStreamDefaultController<Uint8Array> | null = null;
  private encoder = new TextEncoder();
  private unsubscribers: Array<() => void> = [];

  constructor(opts: IgnisGatewayProcessOptions) {
    this.opts = opts;
    this.tag = opts.logTag ?? opts.backend;
    this.channelId = `copilot-${opts.backend}-${Date.now().toString(36)}-${++channelCounter}`;
  }

  start(): { stdin: WritableStream<Uint8Array>; stdout: ReadableStream<Uint8Array> } {
    const wsApi = (window as unknown as { __ignis?: { ws?: IgnisWsApi } }).__ignis?.ws;

    if (!wsApi || typeof wsApi.channel !== "function") {
      throw new Error("Ignis WebSocket API (window.__ignis.ws) is not available.");
    }

    this.channel = wsApi.channel(CHANNEL);

    this.unsubscribers.push(
      this.channel.subscribe("acp-open-ok", (msg) => {
        if (msg.payload?.channelId !== this.channelId) return;
        this.opened = true;
        logInfo(`[AgentMode] gateway session open (pid ${msg.payload?.pid}) [${this.tag}]`);
        for (const line of this.pendingStdin.splice(0)) {
          this.sendStdinLine(line);
        }
      })
    );

    this.unsubscribers.push(
      this.channel.subscribe("acp-stdout", (msg) => {
        if (msg.payload?.channelId !== this.channelId) return;
        const line = msg.payload.line ?? "";
        this.stdoutController?.enqueue(this.encoder.encode(`${line}\n`));
      })
    );

    this.unsubscribers.push(
      this.channel.subscribe("acp-stderr", (msg) => {
        if (msg.payload?.channelId !== this.channelId) return;
        logInfo(`[AgentMode][${this.tag}] ${msg.payload?.line ?? ""}`);
      })
    );

    this.unsubscribers.push(
      this.channel.subscribe("acp-exit", (msg) => {
        if (msg.payload?.channelId !== this.channelId) return;
        this.handleExit(msg.payload?.code ?? null, msg.payload?.signal ?? null, msg.payload?.error);
      })
    );

    const stdout = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.stdoutController = controller;
      },
    });

    const stdin = new WritableStream<Uint8Array>({
      write: (chunk) => {
        this.handleStdinChunk(chunk);
        return Promise.resolve();
      },
    });

    logInfo(
      `[AgentMode] opening gateway transport ${this.opts.backend} (vault=${this.opts.vaultId || "?"}, channelId=${this.channelId}) [${this.tag}]`
    );
    this.channel.send("acp-open", {
      channelId: this.channelId,
      vault: this.opts.vaultId,
      backend: this.opts.backend,
    });

    return { stdin, stdout: sanitizeAcpStdout(stdout) };
  }

  private handleStdinChunk(chunk: Uint8Array) {
    this.stdinBuffer += new TextDecoder().decode(chunk);
    let idx;
    while ((idx = this.stdinBuffer.indexOf("\n")) !== -1) {
      const line = this.stdinBuffer.slice(0, idx).trim();
      this.stdinBuffer = this.stdinBuffer.slice(idx + 1);
      if (!line) continue;
      if (this.opened) {
        this.sendStdinLine(line);
      } else {
        this.pendingStdin.push(line);
      }
    }
  }

  private sendStdinLine(line: string) {
    if (!this.channel || this.exited) return;
    this.channel.send("acp-stdin", { channelId: this.channelId, line });
  }

  private handleExit(code: number | null, signal: string | null, error?: string) {
    if (this.exited) return;
    this.exited = true;
    this.exitCode = code;
    this.exitSignal = signal;

    if (error) {
      logError(`[AgentMode] gateway session failed [${this.tag}]: ${error}`);
    } else {
      logInfo(`[AgentMode] gateway session exited code=${code} signal=${signal} [${this.tag}]`);
    }

    this.stdoutController?.close();
    this.unsubscribers.forEach((off) => off());
    this.unsubscribers = [];

    for (const fn of this.exitListeners) {
      try {
        fn(code, signal);
      } catch (e) {
        logWarn(`[AgentMode] exit listener threw [${this.tag}]`, e);
      }
    }
  }

  onExit(listener: (code: number | null, signal: string | null) => void): () => void {
    if (this.exited) {
      try {
        listener(this.exitCode, this.exitSignal);
      } catch (e) {
        logWarn(`[AgentMode] exit listener threw [${this.tag}]`, e);
      }
      return () => {};
    }
    this.exitListeners.add(listener);
    return () => this.exitListeners.delete(listener);
  }

  isRunning(): boolean {
    return this.opened && !this.exited;
  }

  async shutdown(): Promise<void> {
    if (this.exited || !this.channel) return;

    const exited = new Promise<void>((resolve) => this.onExit(() => resolve()));

    logInfo(`[AgentMode] closing gateway session [${this.tag}]`);
    this.channel.send("acp-close", { channelId: this.channelId });

    const winner = await Promise.race([
      exited.then(() => "exited" as const),
      new Promise<"timeout">((resolve) =>
        window.setTimeout(() => resolve("timeout"), SHUTDOWN_GRACE_MS)
      ),
    ]);

    if (winner === "timeout") {
      logWarn(
        `[AgentMode] gateway session did not exit within ${SHUTDOWN_GRACE_MS}ms [${this.tag}]`
      );
      this.handleExit(null, "timeout", undefined);
    }
  }
}
