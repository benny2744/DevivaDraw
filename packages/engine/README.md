# @benny2744/engine

Framework-agnostic whiteboard core for [Deviva Draw](https://github.com/nhtera/DevivaDraw): the
element model, scene store, history (undo/redo), renderer, geometry, and input/tools state
machine. No DOM/React dependency required at the type level — bring your own `<canvas>` and
pointer events, or use [`@benny2744/react`](https://www.npmjs.com/package/@benny2744/react)
for a ready-made `<DevivaDraw/>` component.

## Install

```bash
npm install @benny2744/engine
```

## Minimal example

```ts
import { Scene, createRectangleElement } from "@benny2744/engine";

const scene = new Scene();
scene.addElement(
  createRectangleElement({ x: 0, y: 0, width: 100, height: 80 }),
);

console.log(scene.getElements());
```

Rendering, input handling, persistence (local storage/PNG/SVG export), and E2E-encrypted share
links are all exposed from the same entry point — see the
[repository](https://github.com/nhtera/DevivaDraw) for the full API surface and the
`packages/react` source for a complete integration.

## License

MIT
