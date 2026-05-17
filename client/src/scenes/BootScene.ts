import Phaser from "phaser"
import { Server } from "../net/Server"

export class BootScene extends Phaser.Scene {
  private server!: Server

  constructor() {
    super("BootScene")
  }

  create(): void {
    const session_token = "xXTheLegend42Xx" // Using Username for now until I am ready to implement auth and login sessions

    this.server = new Server()
    this.server.connect("ws://localhost:8080/ws", session_token)

    this.input.keyboard?.on("keydown-SPACE", () => {
      this.server.send({
        type: "ping",
        time: Date.now(),
      })
    })
  }
}
