import { describe, expect, it, vi } from "vitest";
import { createRectangleElement, Scene } from "@benny2744/engine";
import { encryptEnvelope, generateRoomKey, importRoomKey } from "./message-codec";
import { handleInboundMessage } from "./inbound-message-handler";
import { PresenceStore } from "./presence-state";

async function makeDeps() {
  const scene = new Scene();
  const presence = new PresenceStore();
  const roomKey = await importRoomKey(await generateRoomKey());
  const markSynced = vi.fn();
  const onPeerLeft = vi.fn();
  const onPeerJoined = vi.fn();
  const onSnapshotRequested = vi.fn();
  return { scene, presence, roomKey, markSynced, onPeerLeft, onPeerJoined, onSnapshotRequested };
}

describe("handleInboundMessage — element-delta", () => {
  it("applies a valid encrypted element delta and marks it synced", async () => {
    const deps = await makeDeps();
    const source = new Scene();
    const element = source.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const envelope = await encryptEnvelope(deps.roomKey, "element-delta", { element });

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.scene.getElement(element.id)).toEqual(element);
    expect(deps.markSynced).toHaveBeenCalledWith(element.id, element.version, undefined);
  });

  it("does not mark synced when the remote element loses the LWW comparison", async () => {
    const deps = await makeDeps();
    const local = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const stale = { ...local, version: 0, x: 999 };
    const envelope = await encryptEnvelope(deps.roomKey, "element-delta", { element: stale });

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.scene.getElement(local.id)).toEqual(local);
    expect(deps.markSynced).not.toHaveBeenCalled();
  });
});

describe("handleInboundMessage — snapshot", () => {
  it("applies every element in a valid snapshot payload", async () => {
    const deps = await makeDeps();
    const source = new Scene();
    const a = source.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const b = source.addElement(createRectangleElement({ x: 5, y: 5, width: 1, height: 1 }));
    const envelope = await encryptEnvelope(deps.roomKey, "snapshot", { elements: [a, b] });

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.scene.getElement(a.id)).toEqual(a);
    expect(deps.scene.getElement(b.id)).toEqual(b);
    expect(deps.markSynced).toHaveBeenCalledTimes(2);
  });

  it("ignores a snapshot payload whose elements field isn't an array", async () => {
    const deps = await makeDeps();
    const envelope = await encryptEnvelope(deps.roomKey, "snapshot", { elements: "not-an-array" });
    await expect(handleInboundMessage(JSON.stringify(envelope), deps)).resolves.toBeUndefined();
    expect(deps.markSynced).not.toHaveBeenCalled();
  });
});

describe("handleInboundMessage — presence", () => {
  it("applies a valid presence update tagged with the sender's peerId", async () => {
    const deps = await makeDeps();
    const payload = { name: "Bob", color: "#00ff00", point: { x: 1, y: 1 }, selectedElementIds: [], viewport: null };
    const envelope = { ...(await encryptEnvelope(deps.roomKey, "presence", payload, { compress: false })), peerId: "peer-42" };

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.presence.get("peer-42")).toMatchObject({ name: "Bob" });
  });

  it("ignores a presence envelope missing a peerId", async () => {
    const deps = await makeDeps();
    const payload = { name: "Bob", color: "#00ff00", point: null, selectedElementIds: [], viewport: null };
    const envelope = await encryptEnvelope(deps.roomKey, "presence", payload, { compress: false });

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.presence.list()).toHaveLength(0);
  });
});

describe("handleInboundMessage — peer lifecycle", () => {
  it("routes peer-left to onPeerLeft", async () => {
    const deps = await makeDeps();
    await handleInboundMessage(JSON.stringify({ type: "peer-left", peerId: "peer-1" }), deps);
    expect(deps.onPeerLeft).toHaveBeenCalledWith("peer-1");
  });

  it("routes peer-joined to onPeerJoined, so existing peers republish for the newcomer", async () => {
    const deps = await makeDeps();
    await handleInboundMessage(JSON.stringify({ type: "peer-joined", peerId: "peer-1" }), deps);
    expect(deps.onPeerJoined).toHaveBeenCalledOnce();
    // Nothing is applied to the scene or presence store from an announcement — it carries no state.
    expect(deps.presence.list()).toHaveLength(0);
  });

  it("routes a relay-forwarded snapshot-request to onSnapshotRequested", async () => {
    const deps = await makeDeps();
    await handleInboundMessage(JSON.stringify({ type: "snapshot-request" }), deps);
    expect(deps.onSnapshotRequested).toHaveBeenCalledOnce();
  });
});

describe("handleInboundMessage — hostile/malformed input never throws", () => {
  const cases: Array<[string, string]> = [
    ["not json at all", "{{{"],
    ["a JSON array instead of an object", "[1,2,3]"],
    ["a JSON primitive", '"hello"'],
    ["an object with no type", "{}"],
    ["an object with a non-string type", '{"type":42}'],
    ["an unknown type", '{"type":"delete-everything"}'],
    ["an element-delta with garbage ciphertext", '{"type":"element-delta","iv":"not-real","ciphertext":"also-not-real"}'],
    ["a presence message with no peerId", '{"type":"presence","iv":"x","ciphertext":"y"}'],
  ];

  it.each(cases)("%s", async (_label, raw) => {
    const deps = await makeDeps();
    await expect(handleInboundMessage(raw, deps)).resolves.toBeUndefined();
    expect(deps.scene.getElements()).toHaveLength(0);
    expect(deps.presence.list()).toHaveLength(0);
  });

  it("rejects a wrong-key element-delta without crashing or applying anything", async () => {
    const deps = await makeDeps();
    const wrongKey = await importRoomKey(await generateRoomKey());
    const source = new Scene();
    const element = source.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const envelope = await encryptEnvelope(wrongKey, "element-delta", { element });

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.scene.getElements()).toHaveLength(0);
  });
});

describe("handleInboundMessage — senderRole gating", () => {
  it("calls onElementRefused when a non-admin tries to edit an admin-locked element, and ignores layer adminLocked changes from non-admin snapshots", async () => {
    const onElementRefused = vi.fn();
    const deps = { ...(await makeDeps()), onElementRefused };
    const element = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10 }));
    const locked = deps.scene.updateElement(element.id, { adminLocked: true })!;
    const forged = { ...locked, version: locked.version + 1, x: 999 };
    const envelope = { ...(await encryptEnvelope(deps.roomKey, "element-delta", { element: forged })), senderRole: "editor" };

    await handleInboundMessage(JSON.stringify(envelope), deps);

    expect(deps.scene.getElement(element.id)?.x).toBe(0);
    expect(onElementRefused).toHaveBeenCalledWith(deps.scene, forged, undefined);
  });

  it("refuses non-admin snapshot layer changes that alter adminLocked", async () => {
    const { PageStore } = await import("./page-store");
    const { createPageStoreCollabAdapter } = await import("./page-store-adapter");
    const store = PageStore.fresh();
    const pageId = store.getActivePageId();
    const scene = store.getActiveScene();
    const pages = createPageStoreCollabAdapter(store);
    const deps = { ...(await makeDeps()), scene, pages };

    const layers = scene.getLayers();
    const tamperedManifest = {
      version: 99,
      versionNonce: 1,
      layers: [{ ...layers[0]!, adminLocked: true as const }],
    };
    const envelope = {
      ...(await encryptEnvelope(deps.roomKey, "snapshot", {
        manifest: pages.getManifest(),
        pages: [{ id: pageId, name: "Page 1", elements: [], layers: tamperedManifest }],
      })),
      senderRole: "editor",
    };

    await handleInboundMessage(JSON.stringify(envelope), deps);
    expect(scene.getLayers()[0]!.adminLocked).toBeUndefined();
  });
});

describe("handleInboundMessage — held private-mode frames", () => {
  it("records new elements from held frames and enforces add-only against existing non-held elements", async () => {
    const onElementRefused = vi.fn();
    const deps = { ...(await makeDeps()), isAdmin: true, onElementRefused };
    const existing = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10 }));

    // New element in a held frame -> accepted and marked held.
    const source = new Scene();
    const fresh = source.addElement(createRectangleElement({ x: 20, y: 20, width: 10, height: 10 }));
    const heldNew = { ...(await encryptEnvelope(deps.roomKey, "element-delta", { element: fresh })), senderRole: "editor", held: true };
    await handleInboundMessage(JSON.stringify(heldNew), deps);
    expect(deps.scene.getElement(fresh.id)).toEqual(fresh);
    expect(deps.scene.isElementHeld(fresh.id)).toBe(true);

    // Follow-up edit to that held element -> allowed.
    const freshEdited = source.updateElement(fresh.id, { x: 30 })!;
    const heldEdit = { ...(await encryptEnvelope(deps.roomKey, "element-delta", { element: freshEdited })), senderRole: "editor", held: true };
    await handleInboundMessage(JSON.stringify(heldEdit), deps);
    expect(deps.scene.getElement(fresh.id)?.x).toBe(30);

    // Held frame trying to modify pre-existing non-held element -> refused (add-only).
    const illegalEdit = { ...existing, version: existing.version + 1, x: 999 };
    const heldIllegal = { ...(await encryptEnvelope(deps.roomKey, "element-delta", { element: illegalEdit })), senderRole: "editor", held: true };
    await handleInboundMessage(JSON.stringify(heldIllegal), deps);
    expect(deps.scene.getElement(existing.id)?.x).toBe(0);
    expect(onElementRefused).toHaveBeenCalledWith(deps.scene, illegalEdit, undefined);
  });
});

/**
 * The ghost-move guard: a non-admin's stale element-delta that strictly loses LWW on the ADMIN
 * client must fire `onStaleDelta` (the host floods its newer copy back). Silent drops are what
 * let a classchat student's moved shape stay visible only on their own board. Echoes (same
 * version AND nonce), admin senders, snapshot bulk merges, and non-admin receivers must never
 * trigger it — those are reconciliations, not conflicts to arbitrate.
 */
describe("handleInboundMessage — element-delta stale correction", () => {
  async function makeAdminDeps() {
    return { ...(await makeDeps()), isAdmin: true, onStaleDelta: vi.fn(), onElementRefused: vi.fn() };
  }
  async function editorDelta(deps: Awaited<ReturnType<typeof makeAdminDeps>>, element: unknown) {
    const envelope = await encryptEnvelope(deps.roomKey, "element-delta", { element });
    return JSON.stringify({ ...envelope, senderRole: "editor", peerId: "p1" });
  }

  it("fires onStaleDelta when an editor's older delta loses to the admin's newer copy", async () => {
    const deps = await makeAdminDeps();
    const added = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    deps.scene.updateElement(added.id, { x: 1 }); // admin copy is now ahead
    const current = deps.scene.getElement(added.id)!;
    const stale = { ...current, version: current.version - 1, x: 777 };

    await handleInboundMessage(await editorDelta(deps, stale), deps);

    expect(deps.onStaleDelta).toHaveBeenCalledTimes(1);
    expect(deps.onStaleDelta.mock.calls[0]?.[1]?.id).toBe(added.id);
    expect(deps.scene.getElement(added.id)).toEqual(current); // the stale frame was not applied
    expect(deps.markSynced).not.toHaveBeenCalled();
  });

  it("never fires for an identical echo (same version and versionNonce)", async () => {
    const deps = await makeAdminDeps();
    const added = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const current = deps.scene.getElement(added.id)!;

    await handleInboundMessage(await editorDelta(deps, current), deps);

    expect(deps.onStaleDelta).not.toHaveBeenCalled();
  });

  it("never fires for admin senders (no admin-to-admin ping-pong)", async () => {
    const deps = await makeAdminDeps();
    const added = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    deps.scene.updateElement(added.id, { x: 1 });
    const current = deps.scene.getElement(added.id)!;
    const stale = { ...current, version: current.version - 1, x: 777 };
    const envelope = await encryptEnvelope(deps.roomKey, "element-delta", { element: stale });

    await handleInboundMessage(JSON.stringify({ ...envelope, senderRole: "admin", peerId: "p2" }), deps);

    expect(deps.onStaleDelta).not.toHaveBeenCalled();
  });

  it("never fires during snapshot bulk merges", async () => {
    const deps = await makeAdminDeps();
    const added = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    deps.scene.updateElement(added.id, { x: 1 });
    const current = deps.scene.getElement(added.id)!;
    const stale = { ...current, version: current.version - 1, x: 777 };
    const envelope = await encryptEnvelope(deps.roomKey, "snapshot", { elements: [stale] });

    await handleInboundMessage(JSON.stringify({ ...envelope, senderRole: "editor", peerId: "p1" }), deps);

    expect(deps.onStaleDelta).not.toHaveBeenCalled();
    expect(deps.scene.getElement(added.id)).toEqual(current);
  });

  it("never fires on non-admin receivers (only the admin client arbitrates)", async () => {
    const deps = { ...(await makeDeps()), onStaleDelta: vi.fn() }; // isAdmin absent
    const added = deps.scene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    deps.scene.updateElement(added.id, { x: 1 });
    const current = deps.scene.getElement(added.id)!;
    const stale = { ...current, version: current.version - 1, x: 777 };
    const envelope = await encryptEnvelope(deps.roomKey, "element-delta", { element: stale });

    await handleInboundMessage(JSON.stringify({ ...envelope, senderRole: "editor", peerId: "p1" }), deps);

    expect(deps.onStaleDelta).not.toHaveBeenCalled();
  });
});
