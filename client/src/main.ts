import Phaser from "phaser"
import { BootScene } from "./scenes/BootScene"
import "./style.css"

new Phaser.Game({
  type: Phaser.AUTO,
  parent: "app",
  backgroundColor: "#111827",
  scale: {
    mode: Phaser.Scale.RESIZE,
    width: window.innerWidth,
    height: window.innerHeight
  },
  scene: [BootScene]
})
