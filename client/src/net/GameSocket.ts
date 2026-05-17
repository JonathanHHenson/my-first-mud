// src/net/GameSocket.ts
export class GameSocket {
  private socket?: WebSocket;
  private pingInterval?: number;

  connect(url: string): void {
    this.socket = new WebSocket(url);

    this.socket.onopen = () => {
      console.log("connected");
      this.send({ type: "hello", message: "hello from Phaser" });
    };

    this.socket.onmessage = (event) => {
      console.log("received:", event.data);
    };

    this.socket.onclose = () => {
      console.log("disconnected");
    };

    this.socket.onerror = (event) => {
      console.error("websocket error:", event);
    };
  }

  send(message: unknown): void {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      console.warn("socket not open");
      return;
    }

    this.socket.send(JSON.stringify(message));
  }

  disconnect(): void {
    this.socket?.close();
  }
}
