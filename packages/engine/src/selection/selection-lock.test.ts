import { describe, expect, it } from "vitest";
import { createRectangleElement } from "../elements/shape-elements";
import { Scene } from "../scene/scene";
import { hitTestElement } from "./hit-test";
import { lockForEveryone, lockSelection, unlockForEveryone, unlockSelection } from "./selection-lock";

describe("lockSelection / unlockSelection", () => {
  it("locking a selected element makes it unhittable by hitTestElement", () => {
    const scene = new Scene();
    const rect = scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10, backgroundColor: "#fff" }));
    expect(hitTestElement(scene.getElement(rect.id)!, { x: 5, y: 5 }, 1)).toBe(true);

    lockSelection(scene, [rect.id]);

    expect(scene.getElement(rect.id)?.locked).toBe(true);
    expect(hitTestElement(scene.getElement(rect.id)!, { x: 5, y: 5 }, 1)).toBe(false);
  });

  it("unlocking restores normal hit-testability", () => {
    const scene = new Scene();
    const rect = scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10, backgroundColor: "#fff" }));
    lockSelection(scene, [rect.id]);
    unlockSelection(scene, [rect.id]);
    expect(scene.getElement(rect.id)?.locked).toBe(false);
    expect(hitTestElement(scene.getElement(rect.id)!, { x: 5, y: 5 }, 1)).toBe(true);
  });

  it("is a no-op (no write, no version bump) when already in the target lock state", () => {
    const scene = new Scene();
    const rect = scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10 }));
    const versionBefore = scene.getElement(rect.id)!.version;
    unlockSelection(scene, [rect.id]); // already unlocked
    expect(scene.getElement(rect.id)?.version).toBe(versionBefore);
  });

  it("ignores unknown or deleted ids without throwing", () => {
    const scene = new Scene();
    const rect = scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10 }));
    scene.deleteElement(rect.id);
    expect(() => lockSelection(scene, [rect.id, "missing"])).not.toThrow();
  });
});

describe("lockForEveryone / unlockForEveryone", () => {
  it("sets adminLocked: true and omits the key completely when unlocked", () => {
    const scene = new Scene();
    const rect = scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10, backgroundColor: "#fff" }));
    expect("adminLocked" in scene.getElement(rect.id)!).toBe(false);

    lockForEveryone(scene, [rect.id]);
    const locked = scene.getElement(rect.id)!;
    expect(locked.adminLocked).toBe(true);
    expect(scene.effectiveLocked(locked)).toBe(true);
    expect(hitTestElement(locked, { x: 5, y: 5 }, 1)).toBe(false);

    const versionWhenLocked = locked.version;
    lockForEveryone(scene, [rect.id]);
    expect(scene.getElement(rect.id)!.version).toBe(versionWhenLocked);

    unlockForEveryone(scene, [rect.id]);
    const unlocked = scene.getElement(rect.id)!;
    expect("adminLocked" in unlocked).toBe(false);
    expect(scene.effectiveLocked(unlocked)).toBe(false);
    expect(hitTestElement(unlocked, { x: 5, y: 5 }, 1)).toBe(true);
  });
});
