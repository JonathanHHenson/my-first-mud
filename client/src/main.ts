import Phaser from "phaser";
import { BootScene } from "./scenes/BootScene";
import "./style.css";

new Phaser.Game({
  type: Phaser.AUTO,
  parent: "app",
  width: 960,
  height: 540,
  backgroundColor: "#111827",
  scene: [BootScene]
});
