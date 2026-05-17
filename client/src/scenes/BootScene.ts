import Phaser from "phaser";
import { GameSocket } from "../net/GameSocket";

export class BootScene extends Phaser.Scene {
  private gameSocket!: GameSocket;

  constructor() {
    super("BootScene");
  }

  create(): void {
    this.gameSocket = new GameSocket();
    this.gameSocket.connect("ws://localhost:8080/ws");

    this.input.keyboard?.on("keydown-SPACE", () => {
      this.gameSocket.send({
        type: "ping",
        time: Date.now(),
      });
    });
  }
}
