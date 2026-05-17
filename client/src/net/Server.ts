export class Server {
  private socket?: WebSocket

  connect(url: string, session_token: string): void {
    const url_with_token = new URL(url)
    url_with_token.searchParams.set("token", session_token)

    this.socket = new WebSocket(url_with_token.toString())

    this.socket.onopen = () => {
      console.log("connected")
      this.send({ type: "hello", message: "hello from Phaser" })
    }

    this.socket.onmessage = async (event) => {
      console.log("received:", await event.data.text())
    }

    this.socket.onclose = () => {
      console.log("disconnected")
    }

    this.socket.onerror = (event) => {
      console.error("websocket error:", event)
    }
  }

  send(message: unknown): void {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      console.warn("socket not open")
      return
    }

    this.socket.send(JSON.stringify(message))
  }

  disconnect(): void {
    this.socket?.close()
  }
}
