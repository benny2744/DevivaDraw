import { afterEach, describe, expect, it } from "vitest";
import { createRectangleElement, Scene } from "@benny2744/engine";
import { adoptRoomPages } from "./adopt-room-pages";
import { CollabSession } from "./collab-session";
import { PageStore } from "./page-store";
import { createPageStoreCollabAdapter } from "./page-store-adapter";
import { generateRoomKey } from "./message-codec";
import type { WebSocketLike } from "./connection-manager";

/**
 * A minimal in-memory relay standing in for `apps/collab-server`'s Durable Object room, exercising the
 * full client-side wire protocol end to end (encryption, connection lifecycle, LWW merge) without any
 * real network or Workers runtime — the two-simulated-clients-over-an-in-memory-relay convergence test
 * this phase's spec calls for. The real server's own relay/broadcast/snapshot logic is covered
 * separately and more thoroughly in `apps/collab-server/src/room-connection-registry.test.ts`; this
 * harness only needs to be faithful enough to prove `CollabSession` itself behaves correctly against it.
 */
class FakeRoomRelay {
  private readonly members: Array<{ peerId: string; socket: FakeSocket; role?: "admin" | "editor" | "viewer" }> = [];
  private storedPublicSnapshot: string | null = null;
  private storedAdminSnapshot: string | null = null;
  private nextPeerId = 1;
  private privateMode = false;
  /** Every socket ever created, in creation order — lets a test reach in and simulate a server-side drop on a specific peer's connection (`ConnectionManager` itself has no such hook; only the transport does). */
  readonly sockets: FakeSocket[] = [];

  setPrivateMode(enabled: boolean): void {
    this.privateMode = enabled;
  }

  createSocket(role?: "admin" | "editor" | "viewer"): FakeSocket {
    const peerId = `peer-${this.nextPeerId++}`;
    const socket = new FakeSocket((raw) => this.handleMessage(peerId, socket, role, raw));
    this.members.push({ peerId, socket, role });
    this.sockets.push(socket);
    queueMicrotask(() => socket.open());
    return socket;
  }

  private handleMessage(senderId: string, senderSocket: FakeSocket, senderRole: "admin" | "editor" | "viewer" | undefined, raw: string): void {
    const message = JSON.parse(raw) as { type: string; audience?: "all" | "admins" | "others" };
    if (message.type === "snapshot-request") {
      const requester = this.members.find((m) => m.peerId === senderId);
      const cached = requester?.role === "admin" ? (this.storedAdminSnapshot ?? this.storedPublicSnapshot) : this.storedPublicSnapshot;
      if (cached) senderSocket.deliver(cached);
      else this.broadcastExceptSender(senderId, raw);
      return;
    }
    const isHeld = this.privateMode && senderRole === "editor" && message.type === "element-delta";
    const audience = isHeld ? "admins" : (senderRole === "admin" && message.audience ? message.audience : "all");
    const stamped = JSON.stringify({
      ...message,
      audience: undefined,
      peerId: senderId,
      ...(senderRole ? { senderRole } : {}),
      ...(isHeld ? { held: true } : {}),
    });
    if (message.type === "snapshot") {
      if (audience === "admins") this.storedAdminSnapshot = stamped;
      else if (audience === "all") {
        this.storedPublicSnapshot = stamped;
        this.storedAdminSnapshot = null;
      }
    }
    this.broadcastExceptSender(senderId, stamped, audience);
  }

  private broadcastExceptSender(senderId: string, raw: string, audience: "all" | "admins" | "others" = "all"): void {
    for (const member of this.members) {
      if (member.peerId === senderId) continue;
      if (audience === "admins" && member.role !== "admin") continue;
      if (audience === "others" && member.role === "admin") continue;
      member.socket.deliver(raw);
    }
  }

  /**
   * Simulates the real server noticing a connection died (`RoomDO.webSocketClose` -> `registry.leave`)
   * and forgetting it — removes `socket` from `members` *before* firing its drop, mirroring the real
   * server's behavior of never routing further broadcasts to a connection it already knows is gone.
   * Without this, a dropped-but-not-yet-forgotten member would keep "receiving" broadcasts (including
   * ones the reconnected peer sends to itself under its new identity) via that same still-wired fake
   * socket, a test-harness-only artifact real sockets don't have (a truly dead `WebSocket` never fires
   * another `onmessage`).
   */
  dropConnection(socket: FakeSocket): void {
    const index = this.members.findIndex((member) => member.socket === socket);
    if (index !== -1) this.members.splice(index, 1);
    socket.simulateDrop();
  }
}

class FakeSocket implements WebSocketLike {
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;

  constructor(private readonly onSend: (raw: string) => void) {}

  open(): void {
    this.readyState = 1;
    this.onopen?.();
  }

  deliver(raw: string): void {
    this.onmessage?.({ data: raw });
  }

  send(data: string): void {
    this.onSend(data);
  }

  close(): void {
    this.readyState = 3;
    this.onclose?.({ code: 1000 });
  }

  /** Simulates the underlying transport dropping *without* the caller having asked for it (a server restart, a network blip) — distinct from `close()`, which `ConnectionManager.disconnect()` calls and which must never trigger a reconnect. */
  simulateDrop(): void {
    this.readyState = 3;
    this.onclose?.({ code: 1006 });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Polls `condition` until it's true or `timeoutMs` elapses — avoids brittle fixed-delay waits for the async encrypt/decrypt + debounce pipeline. */
async function waitUntil(condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil: timed out");
    await sleep(10);
  }
}

const sessions: CollabSession[] = [];

/**
 * Stands in for `POST /room`. This harness has no HTTP server (only a socket relay), and the real
 * request is covered in `apps/collab-server`'s route tests plus the opt-in integration suite. The token
 * shape matters here though — `{role}.{mac}` is what `roleClaimedByToken` reads to decide the local
 * role — so the fake mints that shape rather than an arbitrary string.
 */
let mintedRooms = 0;
function fakeCreateRoom(): Promise<{ roomId: string; editorToken: string; viewerToken: string }> {
  const roomId = `room-${++mintedRooms}`;
  return Promise.resolve({ roomId, editorToken: `editor.mac-${roomId}`, viewerToken: `viewer.mac-${roomId}` });
}

/** A multi-page peer, the way both the browser shell and the headless bridge run one. */
function makePagedSession(relay: FakeRoomRelay, store: PageStore, name: string): CollabSession {
  const session = new CollabSession({
    scene: store.getActiveScene(),
    pages: createPageStoreCollabAdapter(store),
    userName: name,
    userColor: "#123456",
    createSocket: () => relay.createSocket(),
    createRoom: fakeCreateRoom,
    initialBackoffMs: 10,
    maxBackoffMs: 30,
  });
  sessions.push(session);
  return session;
}

function makeSession(relay: FakeRoomRelay, scene: Scene, name: string, role?: "admin" | "editor" | "viewer"): CollabSession {
  const session = new CollabSession({
    scene,
    userName: name,
    userColor: "#123456",
    createSocket: () => relay.createSocket(role),
    createRoom: fakeCreateRoom,
    // Fast, deterministic-ish reconnect timing for tests that need to observe a reconnect — production
    // relies on `ConnectionManager`'s own (much larger) defaults.
    initialBackoffMs: 10,
    maxBackoffMs: 30,
  });
  sessions.push(session);
  return session;
}

afterEach(() => {
  for (const session of sessions.splice(0)) session.disconnect();
});

describe("CollabSession — end-to-end over an in-memory relay", () => {
  it("propagates a local edit from one peer to another", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    const element = aliceScene.addElement(createRectangleElement({ x: 1, y: 2, width: 3, height: 4 }));
    await waitUntil(() => bobScene.getElement(element.id) !== undefined);

    expect(bobScene.getElement(element.id)).toEqual(element);
  });

  it("converges to an identical final state under a concurrent edit to the same element", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    const base = aliceScene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    await waitUntil(() => bobScene.getElement(base.id) !== undefined);

    // Both peers edit the same element "simultaneously" (no synchronization between the two calls).
    aliceScene.updateElement(base.id, { x: 10 });
    bobScene.updateElement(base.id, { x: 20 });

    await waitUntil(() => {
      const a = aliceScene.getElement(base.id);
      const b = bobScene.getElement(base.id);
      return !!a && !!b && a.version === b.version && a.versionNonce === b.versionNonce;
    });

    expect(aliceScene.getElement(base.id)).toEqual(bobScene.getElement(base.id));
  });

  it("converges under a delete-vs-edit conflict on the same element", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    const base = aliceScene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    await waitUntil(() => bobScene.getElement(base.id) !== undefined);

    aliceScene.deleteElement(base.id);
    bobScene.updateElement(base.id, { x: 77 });

    await waitUntil(() => {
      const a = aliceScene.getElement(base.id);
      const b = bobScene.getElement(base.id);
      return !!a && !!b && a.version === b.version && a.versionNonce === b.versionNonce;
    });

    expect(aliceScene.getElement(base.id)).toEqual(bobScene.getElement(base.id));
  });

  it("recovers a full snapshot when a peer joins an already-populated room (no lost elements)", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await waitUntil(() => alice.connectionStatus === "connected");

    const a = aliceScene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    const b = aliceScene.addElement(createRectangleElement({ x: 5, y: 5, width: 1, height: 1 }));
    await sleep(200); // let alice's own outbound sync settle before bob joins

    const bobScene = new Scene();
    const bob = makeSession(relay, bobScene, "Bob");
    await bob.joinSession("http://collab.example", roomUrl);

    await waitUntil(() => bobScene.getElement(a.id) !== undefined && bobScene.getElement(b.id) !== undefined);

    expect(bobScene.getElement(a.id)).toEqual(a);
    expect(bobScene.getElement(b.id)).toEqual(b);
  });

  it("propagates throttled cursor presence between peers, tagged with the sender's peer id", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    alice.updateCursor({ x: 42, y: 7 });
    await waitUntil(() => bob.presence.list().length > 0);

    const [presence] = bob.presence.list();
    expect(presence).toMatchObject({ name: "Alice", point: { x: 42, y: 7 } });
  });

  it("clears stale presence after an unexpected drop + reconnect (regression: presence must not survive a drop it never saw a peer-left for)", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    alice.updateCursor({ x: 1, y: 1 });
    await waitUntil(() => bob.presence.list().length > 0);
    expect(bob.presence.list()).toHaveLength(1);

    // Bob's transport drops unexpectedly (not via bob.disconnect()) — simulate on Bob's own socket, the
    // second one created (Alice's is index 0, Bob's is index 1, per creation order in this test).
    const bobSocket = relay.sockets[1]!;
    relay.dropConnection(bobSocket);
    await waitUntil(() => bob.connectionStatus === "connecting");

    // `ConnectionManager` auto-reconnects with the test's fast backoff. `onReconnect` (which clears
    // presence + republishes) fires synchronously inside the same handshake callback that flips status
    // to "connected", so by the time that status is observable, the stale presence is already gone —
    // and nothing in this test causes Alice to resend afterward, so it stays cleared.
    await waitUntil(() => bob.connectionStatus === "connected");
    expect(bob.presence.list()).toHaveLength(0);
  });

  it("stops syncing after disconnect() — no further outbound or inbound traffic", async () => {
    const relay = new FakeRoomRelay();
    const aliceScene = new Scene();
    const bobScene = new Scene();
    const alice = makeSession(relay, aliceScene, "Alice");
    const bob = makeSession(relay, bobScene, "Bob");

    const { editorUrl: roomUrl } = await alice.startSession("http://collab.example", "https://draw.example");
    await bob.joinSession("http://collab.example", roomUrl);
    await waitUntil(() => alice.connectionStatus === "connected" && bob.connectionStatus === "connected");

    bob.disconnect();
    expect(bob.connectionStatus).toBe("disconnected");

    aliceScene.addElement(createRectangleElement({ x: 0, y: 0, width: 1, height: 1 }));
    await sleep(200);
    expect(bobScene.getElements()).toHaveLength(0);
  });

  it("a viewer session sends no element deltas — its own guard, ahead of the relay's", async () => {
    const relay = new FakeRoomRelay();
    const editorScene = new Scene();
    const viewerScene = new Scene();
    const editor = makeSession(relay, editorScene, "Editor");
    const viewer = makeSession(relay, viewerScene, "Viewer");

    const { viewerUrl } = await editor.startSession("http://collab.example", "https://draw.example");
    await viewer.joinSession("http://collab.example", viewerUrl);
    await waitUntil(() => editor.connectionStatus === "connected" && viewer.connectionStatus === "connected");
    expect(viewer.currentRole).toBe("viewer");
    expect(editor.currentRole).toBe("editor");

    // The editor's own edit still reaches the viewer: read-only means it cannot write, not that it is deaf.
    const fromEditor = editorScene.addElement(createRectangleElement({ x: 1, y: 1, width: 1, height: 1 }));
    await waitUntil(() => viewerScene.getElement(fromEditor.id) !== undefined);

    const fromViewer = viewerScene.addElement(createRectangleElement({ x: 9, y: 9, width: 2, height: 2 }));
    await sleep(300); // well past the outbound debounce — nothing should ever arrive
    expect(editorScene.getElement(fromViewer.id)).toBeUndefined();
  });

  it("an editor link joins as an editor, and a link with no token at all still does", async () => {
    const relay = new FakeRoomRelay();
    const scene = new Scene();
    const session = makeSession(relay, scene, "Legacy");
    // A pre-roles link: no `?t=` at all. It must still connect and be able to draw.
    await session.joinSession("http://collab.example", `https://draw.example/room/room-legacy#key=${await generateRoomKey()}`);
    await waitUntil(() => session.connectionStatus === "connected");
    expect(session.currentRole).toBe("editor");
  });

  it("an admin link joins as an admin and syncs element deltas", async () => {
    const relay = new FakeRoomRelay();
    const adminScene = new Scene();
    const peerScene = new Scene();
    const admin = makeSession(relay, adminScene, "Admin");
    const peer = makeSession(relay, peerScene, "Peer");
    const key = await generateRoomKey();

    await admin.joinSession("http://collab.example", `https://draw.example/room/room-admin?t=admin.mac#key=${key}`);
    await peer.joinSession("http://collab.example", `https://draw.example/room/room-admin?t=editor.mac#key=${key}`);
    await waitUntil(() => admin.connectionStatus === "connected" && peer.connectionStatus === "connected");
    expect(admin.currentRole).toBe("admin");

    const element = adminScene.addElement(createRectangleElement({ x: 3, y: 4, width: 5, height: 6 }));
    await waitUntil(() => peerScene.getElement(element.id) !== undefined);
    expect(peerScene.getElement(element.id)).toEqual(element);
  });

  it("admin refuses a non-admin edit to an admin-locked element and snaps the non-admin back", async () => {
    const relay = new FakeRoomRelay();
    const adminScene = new Scene();
    const studentScene = new Scene();
    const admin = makeSession(relay, adminScene, "Admin", "admin");
    const student = makeSession(relay, studentScene, "Student", "editor");
    const key = await generateRoomKey();

    await admin.joinSession("http://collab.example", `https://draw.example/room/room-lock?t=admin.mac#key=${key}`);
    await student.joinSession("http://collab.example", `https://draw.example/room/room-lock?t=editor.mac#key=${key}`);
    await waitUntil(() => admin.connectionStatus === "connected" && student.connectionStatus === "connected");

    const rect = adminScene.addElement(createRectangleElement({ x: 10, y: 20, width: 30, height: 40, adminLocked: true }));
    await waitUntil(() => studentScene.getElement(rect.id)?.adminLocked === true);

    // Simulate a modified student client tampering with the admin-locked element locally.
    studentScene.updateElement(rect.id, { x: 999, adminLocked: undefined });
    await waitUntil(() => studentScene.getElement(rect.id)?.x === 10);

    expect(adminScene.getElement(rect.id)?.x).toBe(10);
    expect(adminScene.getElement(rect.id)?.adminLocked).toBe(true);
    expect(studentScene.getElement(rect.id)?.adminLocked).toBe(true);
  });

  it("private mode hides new student work from peers until private mode ends and locks pre-existing elements on students", async () => {
    const relay = new FakeRoomRelay();
    const adminScene = new Scene();
    const studentAScene = new Scene();
    const studentBScene = new Scene();
    const admin = makeSession(relay, adminScene, "Admin", "admin");
    const studentA = makeSession(relay, studentAScene, "StudentA", "editor");
    const studentB = makeSession(relay, studentBScene, "StudentB", "editor");
    const key = await generateRoomKey();

    await admin.joinSession("http://collab.example", `https://draw.example/room/room-priv?t=admin.mac#key=${key}`);
    await studentA.joinSession("http://collab.example", `https://draw.example/room/room-priv?t=editor.mac#key=${key}`);
    await studentB.joinSession("http://collab.example", `https://draw.example/room/room-priv?t=editor.mac#key=${key}`);
    await waitUntil(() => admin.connectionStatus === "connected" && studentA.connectionStatus === "connected" && studentB.connectionStatus === "connected");

    const shared = adminScene.addElement(createRectangleElement({ x: 0, y: 0, width: 20, height: 20 }));
    await waitUntil(() => studentAScene.getElement(shared.id) !== undefined && studentBScene.getElement(shared.id) !== undefined);

    relay.setPrivateMode(true);
    admin.setPrivateMode(true);
    studentA.setPrivateMode(true);
    studentB.setPrivateMode(true);

    // Pre-existing element acts locked on students.
    expect(studentAScene.effectiveLocked(studentAScene.getElement(shared.id)!)).toBe(true);

    // StudentA draws a new element; admin receives it as held, StudentB does not see it.
    const heldByA = studentAScene.addElement(createRectangleElement({ x: 50, y: 50, width: 15, height: 15 }));
    expect(studentAScene.effectiveLocked(heldByA)).toBe(false);
    await waitUntil(() => adminScene.getElement(heldByA.id) !== undefined);
    expect(adminScene.isElementHeld(heldByA.id)).toBe(true);
    await sleep(150);
    expect(studentBScene.getElement(heldByA.id)).toBeUndefined();

    // Ending private mode releases held work to StudentB and clears heldElementIds.
    relay.setPrivateMode(false);
    studentA.setPrivateMode(false);
    studentB.setPrivateMode(false);
    admin.setPrivateMode(false);

    await waitUntil(() => studentBScene.getElement(heldByA.id) !== undefined);
    expect(adminScene.hasHeldElements()).toBe(false);
    expect(studentAScene.effectiveLocked(studentAScene.getElement(shared.id)!)).toBe(false);
  });

  it("hiding a layer sends tombstones to non-admins and revealing bumps versions so everyone sees it again", async () => {
    const relay = new FakeRoomRelay();
    const adminStore = PageStore.fresh();
    const studentStore = PageStore.fresh();
    const admin = new CollabSession({
      scene: adminStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(adminStore),
      userName: "Admin",
      userColor: "#111111",
      createSocket: () => relay.createSocket("admin"),
      createRoom: fakeCreateRoom,
    });
    const student = new CollabSession({
      scene: studentStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(studentStore),
      userName: "Student",
      userColor: "#222222",
      createSocket: () => relay.createSocket("editor"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(admin, student);

    const key = await generateRoomKey();
    await admin.joinSession("http://collab.example", `https://draw.example/room/room-layers?t=admin.mac#key=${key}`);
    await student.joinSession("http://collab.example", `https://draw.example/room/room-layers?t=editor.mac#key=${key}`);
    await waitUntil(() => admin.connectionStatus === "connected" && student.connectionStatus === "connected");

    const adminScene = adminStore.getActiveScene();
    const pageId = adminStore.getActivePageId();
    const secretLayer = adminScene.addLayer("Answer Key");
    adminScene.setActiveLayer(secretLayer.id);
    const answer = adminScene.addElement(createRectangleElement({ x: 10, y: 10, width: 20, height: 20 }));

    // Force a snapshot so student learns the layer list too, and receives the element while layer is public.
    await waitUntil(() => studentStore.getSceneById(pageId)?.getElement(answer.id)?.isDeleted === false);

    // Admin hides the layer -> students receive a tombstone, admin keeps the real element at the bumped version.
    adminScene.setLayerAudience(secretLayer.id, "admins");
    await waitUntil(() => studentStore.getSceneById(pageId)?.getElement(answer.id)?.isDeleted === true);
    expect(adminScene.getElement(answer.id)?.isDeleted).toBe(false);

    // Late-joining viewer gets only the public snapshot (no answer, no secretLayer).
    const lateViewerStore = PageStore.fresh();
    const lateViewer = new CollabSession({
      scene: lateViewerStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(lateViewerStore),
      userName: "Viewer",
      userColor: "#333333",
      createSocket: () => relay.createSocket("viewer"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(lateViewer);
    await lateViewer.joinSession("http://collab.example", `https://draw.example/room/room-layers?t=viewer.mac#key=${key}`);
    await waitUntil(() => lateViewerStore.getSceneById(pageId) !== null);
    await sleep(100);
    expect(lateViewerStore.getSceneById(pageId)?.getElement(answer.id)).toBeUndefined();
    expect(lateViewerStore.getSceneById(pageId)?.getLayers().some((l) => l.id === secretLayer.id)).toBe(false);

    // Late-joining second admin gets the admin snapshot (both secretLayer and answer).
    const lateAdminStore = PageStore.fresh();
    const lateAdmin = new CollabSession({
      scene: lateAdminStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(lateAdminStore),
      userName: "Admin2",
      userColor: "#444444",
      createSocket: () => relay.createSocket("admin"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(lateAdmin);
    await lateAdmin.joinSession("http://collab.example", `https://draw.example/room/room-layers?t=admin.mac#key=${key}`);
    await waitUntil(() => lateAdminStore.getSceneById(pageId)?.getElement(answer.id)?.isDeleted === false);
    expect(lateAdminStore.getSceneById(pageId)?.getLayers().some((l) => l.id === secretLayer.id && l.audience === "admins")).toBe(true);

    // Admin reveals the layer -> version bumps again and beats the tombstone on student and viewer.
    adminScene.setLayerAudience(secretLayer.id, undefined);
    await waitUntil(() => studentStore.getSceneById(pageId)?.getElement(answer.id)?.isDeleted === false);
    await waitUntil(() => lateViewerStore.getSceneById(pageId)?.getElement(answer.id)?.isDeleted === false);
  });

  it("publishes a layer lock on the next flush, and a student with a newer layer version still adopts it", async () => {
    const relay = new FakeRoomRelay();
    const adminStore = PageStore.fresh();
    const studentStore = PageStore.fresh();
    const admin = new CollabSession({
      scene: adminStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(adminStore),
      userName: "Admin",
      userColor: "#111111",
      createSocket: () => relay.createSocket("admin"),
      createRoom: fakeCreateRoom,
    });
    const student = new CollabSession({
      scene: studentStore.getActiveScene(),
      pages: createPageStoreCollabAdapter(studentStore),
      userName: "Student",
      userColor: "#222222",
      createSocket: () => relay.createSocket("editor"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(admin, student);

    const key = await generateRoomKey();
    await admin.joinSession("http://collab.example", `https://draw.example/room/room-lock?t=admin.mac#key=${key}`);
    await student.joinSession("http://collab.example", `https://draw.example/room/room-lock?t=editor.mac#key=${key}`);
    await waitUntil(() => admin.connectionStatus === "connected" && student.connectionStatus === "connected");

    const adminScene = adminStore.getActiveScene();
    const pageId = adminStore.getActivePageId();
    const layerId = adminScene.getLayers()[0]!.id;
    adminScene.renameLayer(layerId, "Board");
    await waitUntil(() => studentStore.getSceneById(pageId)?.getLayers()[0]?.name === "Board");

    const studentScene = studentStore.getSceneById(pageId)!;
    for (let i = 0; i < 4; i++) studentScene.setLayerVisible(layerId, i % 2 === 0);
    const studentVersion = studentScene.getLayersManifest().version;
    expect(studentVersion).toBeGreaterThan(adminScene.getLayersManifest().version);

    adminScene.setLayerAdminLocked(layerId, true);
    await waitUntil(() => studentStore.getSceneById(pageId)?.getLayers()[0]?.adminLocked === true);
    expect(studentStore.getSceneById(pageId)?.getLayersManifest().version).toBe(studentVersion);
    expect(studentStore.getSceneById(pageId)?.getLayers()[0]?.name).toBe("Board");
  });

  it("tombstones a new student element drawn on an admin-locked layer for every peer", async () => {
    const relay = new FakeRoomRelay();
    const adminStore = PageStore.fresh();
    const studentAStore = PageStore.fresh();
    const studentBStore = PageStore.fresh();
    const open = (store: PageStore, name: string, role: "admin" | "editor", color: string) => {
      const session = new CollabSession({
        scene: store.getActiveScene(),
        pages: createPageStoreCollabAdapter(store),
        userName: name,
        userColor: color,
        createSocket: () => relay.createSocket(role),
        createRoom: fakeCreateRoom,
      });
      sessions.push(session);
      return session;
    };
    const admin = open(adminStore, "Admin", "admin", "#111111");
    const studentA = open(studentAStore, "StudentA", "editor", "#222222");
    const studentB = open(studentBStore, "StudentB", "editor", "#333333");

    const key = await generateRoomKey();
    const url = (role: string) => `https://draw.example/room/room-revoke?t=${role}.mac#key=${key}`;
    await admin.joinSession("http://collab.example", url("admin"));
    await studentA.joinSession("http://collab.example", url("editor"));
    await studentB.joinSession("http://collab.example", url("editor"));
    await waitUntil(
      () => admin.connectionStatus === "connected" && studentA.connectionStatus === "connected" && studentB.connectionStatus === "connected"
    );

    const adminScene = adminStore.getActiveScene();
    const pageId = adminStore.getActivePageId();
    const layerId = adminScene.getLayers()[0]!.id;
    adminScene.setLayerAdminLocked(layerId, true);
    await waitUntil(() => studentAStore.getSceneById(pageId)?.getLayers().some((layer) => layer.id === layerId && layer.adminLocked === true));
    await waitUntil(() => studentBStore.getSceneById(pageId)?.getLayers().some((layer) => layer.id === layerId && layer.adminLocked === true));

    const studentScene = studentAStore.getSceneById(pageId)!;
    const stroke = studentScene.addElement(createRectangleElement({ x: 40, y: 40, width: 12, height: 12 }));

    await waitUntil(() => adminScene.getElement(stroke.id)?.isDeleted === true);
    await waitUntil(() => studentAStore.getSceneById(pageId)?.getElement(stroke.id)?.isDeleted === true);
    await waitUntil(() => studentBStore.getSceneById(pageId)?.getElement(stroke.id)?.isDeleted === true);
  });
});

/**
 * The page list a peer ends up with after joining somebody else's room.
 *
 * The unit tests for `adoptRoomPages` simulate the room's pages arriving by calling `addPage`; this
 * drives the real thing — manifest sync over the relay — because the failure this guards against was
 * not in the helper at all. It shipped in v0.11.0: adoption was wired into the one join path a room
 * *URL* takes, so joining by pasting a link into the dialog skipped it entirely and the joiner kept
 * its own untouched starter page beside the room's. Invisible on the web, where a room is a route;
 * unavoidable on the desktop, where pasting into the dialog is the only way to join.
 */
describe("a peer joining somebody else's room", () => {
  it("ends up with the room's pages, not the room's plus its own starter page", async () => {
    const relay = new FakeRoomRelay();
    const hostStore = PageStore.fresh();
    const hostPageId = hostStore.getActivePageId();
    hostStore.getActiveScene().addElement(createRectangleElement({ x: 0, y: 0, width: 10, height: 10 }));
    const host = makePagedSession(relay, hostStore, "Host");
    const links = await host.startSession("http://relay.test", "http://app.test");

    const joinerStore = PageStore.fresh();
    const joinerStarterId = joinerStore.getActivePageId();
    const preJoinPageIds = new Set(joinerStore.getPages().map((page) => page.id));
    const joiner = makePagedSession(relay, joinerStore, "Joiner");
    await joiner.joinSession("http://relay.test", links.editorUrl);
    await adoptRoomPages(joinerStore, { preJoinPageIds, preJoinActiveId: joinerStarterId, timeoutMs: 2_000 });

    expect(joinerStore.getPages().map((page) => page.id)).toEqual([hostPageId]);
    expect(joinerStore.getActivePageId()).toBe(hostPageId);
  });
});

/**
 * Classchat shared-mode repro: the teacher (admin) draws, two students (collaborator tokens =
 * client `editor` role, relay-stamped `editor`) join, and one student moves a teacher-created
 * element. Every peer must see the move. This is the reported production symptom stated as a test:
 * the mover's own board updates, the others do not.
 */
describe("a collaborator moving an admin's element", () => {
  it("propagates the move to the admin and the other collaborator", async () => {
    const relay = new FakeRoomRelay();
    const key = await generateRoomKey();
    const teacherStore = PageStore.fresh();
    const aStore = PageStore.fresh();
    const bStore = PageStore.fresh();
    const mk = (store: PageStore, role: "admin" | "editor", name: string): CollabSession => {
      const s = new CollabSession({
        scene: store.getActiveScene(),
        pages: createPageStoreCollabAdapter(store),
        userName: name,
        userColor: "#123456",
        createSocket: () => relay.createSocket(role),
        createRoom: fakeCreateRoom,
      });
      sessions.push(s);
      return s;
    };
    const teacher = mk(teacherStore, "admin", "Teacher");
    const studentA = mk(aStore, "editor", "Student A");
    const studentB = mk(bStore, "editor", "Student B");
    await teacher.joinSession("http://relay.test", `https://draw.test/room/cbrepro?t=admin.mac#key=${key}`);
    await waitUntil(() => teacher.connectionStatus === "connected");

    const aPre = new Set(aStore.getPages().map((p) => p.id));
    const aActive0 = aStore.getActivePageId();
    const bPre = new Set(bStore.getPages().map((p) => p.id));
    const bActive0 = bStore.getActivePageId();
    await studentA.joinSession("http://relay.test", `https://draw.test/room/cbrepro?t=editor.mac#key=${key}`);
    await studentB.joinSession("http://relay.test", `https://draw.test/room/cbrepro?t=editor.mac#key=${key}`);
    await Promise.all([
      adoptRoomPages(aStore, { preJoinPageIds: aPre, preJoinActiveId: aActive0, timeoutMs: 2_000 }),
      adoptRoomPages(bStore, { preJoinPageIds: bPre, preJoinActiveId: bActive0, timeoutMs: 2_000 }),
    ]);

    // The teacher draws a shape.
    const teacherScene = teacherStore.getActiveScene();
    const pageId = teacherStore.getActivePageId();
    const shape = teacherScene.addElement(createRectangleElement({ x: 10, y: 10, width: 40, height: 40 }));
    await waitUntil(() => aStore.getSceneById(pageId)?.getElement(shape.id) !== undefined && bStore.getSceneById(pageId)?.getElement(shape.id) !== undefined);

    // Student A moves it (the selection-move gesture commits via scene.updateElement).
    const aScene = aStore.getSceneById(pageId)!;
    aScene.updateElement(shape.id, { x: 500, y: 600 });

    // The teacher and student B must see the new position.
    await waitUntil(() => teacherScene.getElement(shape.id)!.x === 500 && teacherScene.getElement(shape.id)!.y === 600, 3_000);
    await waitUntil(() => {
      const el = bStore.getSceneById(pageId)?.getElement(shape.id);
      return el !== undefined && el.x === 500 && el.y === 600;
    }, 3_000);
  });
});

/**
 * The classchat ghost-move, end to end: a student's copy goes stale (the relay's cached snapshot
 * predates the teacher's edits, exactly the reconnect-after-30s-window shape of the production
 * report), they move the shape, and their delta then strictly loses LWW on the teacher's client.
 * Without the admin's stale-delta flood-back the mover holds a ghost forever — nothing else
 * republishes the authoritative frame. The version floor asserts the snap-back came from the
 * correction flood, not an accidental snapshot delivery.
 */
describe("admin stale-delta flood-back", () => {
  it("snaps a ghost-mover back to the authoritative position", async () => {
    const relay = new FakeRoomRelay();
    const key = await generateRoomKey();
    const teacherScene = new Scene();
    const teacher = new CollabSession({
      scene: teacherScene,
      userName: "Teacher",
      userColor: "#111111",
      createSocket: () => relay.createSocket("admin"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(teacher);
    await teacher.joinSession("http://relay.test", `https://draw.test/room/ghost?t=admin.mac#key=${key}`);
    await waitUntil(() => teacher.connectionStatus === "connected");

    // The teacher draws; an incidental snapshot-request seeds the RELAY'S CACHE at this state —
    // what a rejoiner will be served, stale or not (classchat's 30-second snapshot cadence is what
    // opens the real-world window this models).
    const rect = teacherScene.addElement(createRectangleElement({ x: 10, y: 10, width: 40, height: 40 }));
    const seeder = relay.createSocket("viewer");
    await waitUntil(() => seeder.readyState === 1);
    seeder.send(JSON.stringify({ type: "snapshot-request" }));
    await sleep(100); // teacher answers, relay caches the snapshot (@v1, x=10)
    const frameAtV1 = { ...teacherScene.getElement(rect.id)! };

    // The teacher keeps working — the room advances to @v3 x=999 with the student not yet present.
    teacherScene.updateElement(rect.id, { x: 400 });
    teacherScene.updateElement(rect.id, { x: 999 });
    await waitUntil(() => teacherScene.getElement(rect.id)!.version >= 3);
    await sleep(300); // let the teacher's debounced flushes drain
    const versionBeforeGhostMove = teacherScene.getElement(rect.id)!.version;

    // The remount analog: a FRESH session on the student's stale copy (what WhiteboardViewer's
    // key={roomUrl} remount produces — page store kept, collab session rebuilt). It is served the
    // stale cached snapshot, then "moves" the shape locally — the frame that starts the ghost.
    const studentScene = new Scene();
    studentScene.applyRemoteElement(frameAtV1);
    const student = new CollabSession({
      scene: studentScene,
      userName: "Ghost",
      userColor: "#222222",
      createSocket: () => relay.createSocket("editor"),
      createRoom: fakeCreateRoom,
    });
    sessions.push(student);
    await student.joinSession("http://relay.test", `https://draw.test/room/ghost?t=editor.mac#key=${key}`);
    await waitUntil(() => student.connectionStatus === "connected" && studentScene.getElement(rect.id)?.x === 10);
    studentScene.updateElement(rect.id, { x: 500 });

    // Pre-fix this timed out at x=500 forever: the teacher's client dropped the losing frame with
    // no reply. Post-fix the stale-delta correction floods x=999 at a version past both frames.
    await waitUntil(() => studentScene.getElement(rect.id)!.x === 999, 3_000);
    expect(teacherScene.getElement(rect.id)!.x).toBe(999);
    expect(teacherScene.getElement(rect.id)!.version).toBeGreaterThan(versionBeforeGhostMove); // correction, not luck
  });
});
