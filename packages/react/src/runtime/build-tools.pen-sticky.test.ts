/**
 * The pen must stay armed after a stroke commits.
 *
 * This guards the production defect where each stylus stroke disarmed the pen (the shape tools'
 * "draw then immediately adjust" auto-revert to select was shared by `FreedrawTool` through
 * `shapeToolDeps.onCreated`), forcing students to re-tap the pen between every stroke. Shapes keep
 * the auto-revert — the reported fix must not regress them.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { Scene } from "@benny2744/engine";
import type { Camera } from "@benny2744/engine";
import { buildTools } from "./build-tools";
import { FREEDRAW_TOOL_NAME, HIGHLIGHTER_TOOL_NAME, RECTANGLE_TOOL_NAME, SELECT_TOOL_NAME } from "./tool-names";

const NO_MODS = { shift: false, alt: false, ctrl: false, meta: false };
const CAMERA: Camera = { scrollX: 0, scrollY: 0, zoom: 1 };

function fakeDocument() {
  const ctx = {
    font: "",
    measureText: (text: string) => ({ width: text.length * 8 }),
  };
  return {
    createElement: (tag: string) => (tag === "canvas" ? { getContext: () => ctx } : {}),
  };
}

function build(toolLock: boolean) {
  vi.stubGlobal("document", fakeDocument());
  try {
    const scene = new Scene();
    const container = { clientWidth: 800, clientHeight: 600 } as unknown as HTMLElement;
    const built = buildTools(container, scene, () => CAMERA, () => {}, () => "light", () => toolLock);
    return { scene, ...built };
  } finally {
    vi.unstubAllGlobals();
  }
}

function drawStroke(built: ReturnType<typeof build>, toolName: string): void {
  const machine = built.toolStateMachine;
  expect(machine.setTool(toolName)).toBe(true);
  machine.dispatchGestureStart({ x: 10, y: 10 }, NO_MODS, 0.6, "pen");
  machine.dispatchGestureMove({ x: 40, y: 30 }, NO_MODS, 0.6, "pen");
  machine.dispatchGestureMove({ x: 70, y: 55 }, NO_MODS, 0.6, "pen");
  machine.dispatchGestureEnd({ x: 90, y: 70 }, NO_MODS, 0.6, "pen");
}

function drawRectangle(built: ReturnType<typeof build>): void {
  const machine = built.toolStateMachine;
  expect(machine.setTool(RECTANGLE_TOOL_NAME)).toBe(true);
  machine.dispatchGestureStart({ x: 0, y: 0 }, NO_MODS, undefined, "mouse");
  machine.dispatchGestureMove({ x: 60, y: 40 }, NO_MODS, undefined, "mouse");
  machine.dispatchGestureEnd({ x: 120, y: 80 }, NO_MODS, undefined, "mouse");
}

describe("buildTools — pen stickiness", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("keeps the pen armed after a committed stroke (tool lock off)", () => {
    const built = build(false);
    drawStroke(built, FREEDRAW_TOOL_NAME);
    expect(built.scene.getElements().some((element) => element.type === "freedraw")).toBe(true);
    expect(built.toolStateMachine.getActiveToolName()).toBe(FREEDRAW_TOOL_NAME);
  });

  it("keeps the highlighter armed after a committed stroke (tool lock off)", () => {
    const built = build(false);
    drawStroke(built, HIGHLIGHTER_TOOL_NAME);
    expect(built.toolStateMachine.getActiveToolName()).toBe(HIGHLIGHTER_TOOL_NAME);
  });

  it("still reverts a committed rectangle to the select tool", () => {
    const built = build(false);
    drawRectangle(built);
    expect(built.scene.getElements().length).toBeGreaterThan(0);
    expect(built.toolStateMachine.getActiveToolName()).toBe(SELECT_TOOL_NAME);
  });
});

