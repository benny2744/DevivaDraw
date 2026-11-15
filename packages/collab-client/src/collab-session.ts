/**
 * `CollabSession` — the client-side orchestrator wiring `connection-manager.ts` (transport),
 * `message-codec.ts` (E2E crypto), `inbound-message-handler.ts`/`lww-merge.ts` (conflict resolution),
 * `presence-broadcaster.ts` (outbound presence), and `presence-state.ts` (inbound presence) to a live
 * `@benny2744/engine` `Scene`. Collaboration is opt-in at this class's boundary: a `Scene` never
 * touched by a connected `CollabSession` behaves exactly as it always has.
 *
 * Outbound element sync is diffed by version, not flagged by origin: `syncedVersions` records the last
 * version of each element already sent (locally, or already received and applied remotely — stamped by
 * `inbound-message-handler.ts`'s `markSynced` the instant it's applied). The debounced outbound scan
 * (`outbound-sync.ts`'s `flushElementDeltas`) only sends elements whose current version doesn't match
 * that record — this prevents an echo loop without a fragile "am I applying a remote change right now"
 * flag; the relay's own broadcast-except-sender is the primary echo guard, this is a redundant-resend one.
 *
 * Presence resync on reconnect: `ConnectionManager.onReconnect` fires only after an *unexpected* drop
 * (not the initial connect). On that signal this session clears its inbound `presence` store — a peer
 * that left mid-disconnect was never seen leaving, since this client wasn't connected to receive that
 * `peer-left` — and republishes its own outbound presence so other peers stop seeing a stale cursor.
 */
import { generateRoomKey, importRoomKey } from "./message-codec";
import { ConnectionManager } from "./connection-manager";
import type { GiveUpReason, WebSocketLike } from "./connection-manager";
import { handleInboundMessage } from "./inbound-message-handler";
import { flushCommentDeltas, flushElementDeltas, sendDocumentSnapshot, sendFullSnapshot } from "./outbound-sync";
import type { CollabPagesAdapter } from "./pages-adapter";
import { PresenceBroadcaster } from "./presence-broadcaster";
import { PresenceStore } from "./presence-state";
import type { PresenceViewport } from "./presence-state";
import { buildRoomUrl, buildRoomWebSocketUrl, parseRoomUrl, roleClaimedByToken } from "./room-url";
import { randomVersionNonce } from "@benny2744/engine";
import type { AnyElement, Scene } from "@benny2744/engine";
import { remoteElementWins } from "./lww-merge";

export type CollabConnectionStatus = "disconnected" | "connecting" | "connected";

/** What this client may do in the room it is connected to. Reflects the token it holds; the relay is the authority (see `apps/collab-server`'s `room-connection-registry.ts`). */
export type CollabRole = "admin" | "editor" | "viewer";

export interface StartSessionOptions {
  /**
   * A room minted somewhere other than `POST /room`. The desktop app's LAN host mints its room in
   * Rust, where that relay's token secret lives and never leaves — so there is no request to make, and
   * requiring one would mean the app needed permission to speak HTTP to a local address purely to ask
   * itself for a room id.
   */
  room?: MintedRoom;
  /** Written into both links so a peer opening one knows which relay to connect to. Only for a self-hosted room; omitted for the configured relay, whose address every client already holds. */
  relayBaseUrl?: string;
}

/** The two links a fresh session produces. They address the same room with the same key — only the role token differs. */
export interface RoomLinks {
  editorUrl: string;
  viewerUrl: string;
}

/** `POST /room`'s response body. */
export interface MintedRoom {
  roomId: string;
  editorToken: string;
  viewerToken: string;
}

export interface CollabSessionOptions {
  scene: Scene;
  /** Multi-page host: element sync spans every page (deltas tagged with their `pageId`), the page list itself syncs as an LWW manifest riding on snapshots, and `scene` becomes only the landing spot for legacy un-tagged traffic. See `pages-adapter.ts`. */
  pages?: CollabPagesAdapter;
  userName: string;
  userColor: string;
  /** Overrides the WebSocket factory (`connection-manager.ts`) — tests only. */
  createSocket?(url: string): WebSocketLike;
  /**
   * Overrides the `POST /room` call `startSession` makes — tests only, and for the same reason
   * `createSocket` exists: the unit suite drives a fake in-memory relay with no HTTP server behind it.
   * The real request path is covered by `apps/collab-server`'s route tests and by the opt-in
   * `live-relay.integration.test.ts`, which runs both halves against an actual `wrangler dev`.
   */
  createRoom?(apiBaseUrl: string): Promise<MintedRoom>;
  onStatusChange?(status: CollabConnectionStatus): void;
  /**
   * Fires once when a session ends on its own rather than because the user left — the host stopped
   * hosting, the relay refused this client, or it became unreachable. A status change alone cannot
   * carry this: "disconnected" is also what leaving looks like, and the two need different words on
   * screen.
   */
  onEnded?(reason: GiveUpReason): void;
  /** Overrides `ConnectionManager`'s reconnect-backoff timing — tests only (production relies on the defaults). */
  initialBackoffMs?: number;
  maxBackoffMs?: number;
}

const OUTBOUND_SYNC_DEBOUNCE_MS = 80;
const CURSOR_THROTTLE_MS = 50;
const PRESENCE_IDLE_TICK_MS = 2_000;
/** Bounds how stale a rehydrated room can be after every client disconnects — see `apps/collab-server`'s snapshot-persistence doc for the server side of this same bound. */
const SNAPSHOT_INTERVAL_MS = 30_000;

export class CollabSession {
  readonly presence = new PresenceStore();

  private readonly scene: Scene;
  private readonly pages?: CollabPagesAdapter;
  private readonly createSocket?: (url: string) => WebSocketLike;
  private readonly createRoom?: (apiBaseUrl: string) => Promise<MintedRoom>;
  private readonly onStatusChange?: (status: CollabConnectionStatus) => void;
  private readonly onEnded?: (reason: GiveUpReason) => void;
  private readonly initialBackoffMs?: number;
  private readonly maxBackoffMs?: number;
  private readonly presenceBroadcaster: PresenceBroadcaster;

  private connection: ConnectionManager | null = null;
  private roomKey: CryptoKey | null = null;
  private roomId: string | null = null;
  private status: CollabConnectionStatus = "disconnected";
  private role: CollabRole = "editor";
  private tornDown = true;

  private readonly syncedVersions = new Map<string, number>();
  /** Comment-record sync state, kept apart from `syncedVersions` on purpose — see `outbound-sync.ts`'s `flushCommentDeltas` for why a shared map guarded by a naming convention is a collision waiting to happen. */
  private readonly syncedComments = new Map<string, number>();
  private outboundTimer: ReturnType<typeof setTimeout> | null = null;
  private unsubscribeScene: (() => void) | null = null;
  private unsubscribePages: (() => void) | null = null;
  private readonly pageSceneUnsubs = new Map<string, () => void>();
  private manifestDirty = false;
  /** An admin changed the layer list (lock, audience, rename, reorder). The next flush publishes a snapshot instead of waiting for the 30s timer. */
  private layerManifestDirty = false;
  private readonly knownLayerAudiences = new Map<string, Map<string, "admins" | undefined>>();
  /** Last seen `Scene.getLayersVersion()` per page, so a layer-list edit is distinguishable from an element edit on the same notify. */
  private readonly knownLayerVersions = new Map<string, number>();
  private readonly knownPublicSynced = new Set<string>();
  private readonly pendingHideTombstones = new Set<string>();
  private disposeIdleTicking: (() => void) | null = null;
  private snapshotTimer: ReturnType<typeof setInterval> | null = null;
  private followedPeerId: string | null = null;
  private privateModeEnabled = false;

  constructor(options: CollabSessionOptions) {
    this.scene = options.scene;
    this.pages = options.pages;
    this.createSocket = options.createSocket;
    this.createRoom = options.createRoom;
    this.onStatusChange = options.onStatusChange;
    this.onEnded = options.onEnded;
    this.initialBackoffMs = options.initialBackoffMs;
    this.maxBackoffMs = options.maxBackoffMs;
    this.presenceBroadcaster = new PresenceBroadcaster({
      userName: options.userName,
      userColor: options.userColor,
      throttleMs: CURSOR_THROTTLE_MS,
      getSendDeps: () => {
        const live = this.liveConnection();
        return live ? { roomKey: live.roomKey, send: (data: string) => live.connection.send(data) } : null;
      },
    });
  }

  /**
   * A `{roomKey, connection}` snapshot captured *at call time* — every outbound send path (element
   * deltas, full snapshots, presence) needs one of these, not a live re-read of `this.roomKey`/
   * `this.connection` from inside an async closure: every one of those sends awaits an encrypt step
   * before actually writing to the socket, and `disconnect()` can null both fields out during that gap
   * (test cleanup, a user leaving mid-send). A live re-read would crash on `null!.send(...)`; a snapshot
   * just harmlessly sends over the connection that was actually valid when the call was made.
   */
  private liveConnection(): { roomKey: CryptoKey; connection: ConnectionManager } | null {
    const { roomKey, connection } = this;
    return roomKey && connection ? { roomKey, connection } : null;
  }

  get connectionStatus(): CollabConnectionStatus {
    return this.status;
  }
  get currentRoomId(): string | null {
    return this.roomId;
  }

  /**
   * Starts a brand-new session and connects to it as its editor. Returns both shareable links: the
   * editor link, and a viewer link that joins the same room read-only.
   *
   * The room id now comes from the server rather than `crypto.randomUUID()` here, because the role
   * tokens have to be signed by a secret this client does not hold — an id a client mints has nothing
   * to sign against. The key is still generated locally and still travels only in the URL fragment,
   * so what the server learns by minting a room is exactly what it learned before: a random string.
   */
  async startSession(apiBaseUrl: string, origin: string, options: StartSessionOptions = {}): Promise<RoomLinks> {
    const room = options.room ?? (await (this.createRoom ? this.createRoom(apiBaseUrl) : requestRoom(apiBaseUrl)));
    const keyBase64Url = await generateRoomKey();
    await this.connectToRoom(apiBaseUrl, room.roomId, keyBase64Url, room.editorToken);
    const { relayBaseUrl } = options;
    return {
      editorUrl: buildRoomUrl({ origin, roomId: room.roomId, keyBase64Url, token: room.editorToken, relayBaseUrl }),
      viewerUrl: buildRoomUrl({ origin, roomId: room.roomId, keyBase64Url, token: room.viewerToken, relayBaseUrl }),
    };
  }

  /**
   * Joins an existing session from a room URL another peer shared (`room-url.ts`'s scheme). Throws on a
   * URL that doesn't parse — the caller (UI layer) surfaces that as a validation error.
   *
   * A link to a self-hosted room names its own relay, and that wins over `apiBaseUrl`: the whole point
   * of such a link is that it reaches a machine this client had no way to know about. `room-url.ts`
   * has already restricted that field to local-network addresses, so the override cannot redirect this
   * client somewhere arbitrary.
   */
  async joinSession(apiBaseUrl: string, roomUrl: string): Promise<void> {
    const parsed = new URL(roomUrl);
    const result = parseRoomUrl(parsed.pathname, parsed.hash, parsed.search);
    if (!result.ok) throw new Error(`collab-session: ${result.error}`);
    const { roomId, keyBase64Url, token, relayBaseUrl } = result.value;
    await this.connectToRoom(relayBaseUrl ?? apiBaseUrl, roomId, keyBase64Url, token);
  }

  /**
   * What this client may do in the room. Derived from which token it joined with, so it is a
   * reflection, not a permission: the relay enforces the same thing from the same token, and a client
   * that lied to itself here would simply have its writes dropped. The UI reads it to render a
   * read-only session honestly rather than letting a viewer draw into a void.
   */
  get currentRole(): CollabRole {
    return this.role;
  }

  /** Tears the session down: closes the socket, stops every timer, clears presence. Safe to call whether or not a session is active. */
  disconnect(): void {
    this.tornDown = true;
    this.connection?.disconnect();
    this.connection = null;
    this.unsubscribeScene?.();
    this.unsubscribeScene = null;
    this.unsubscribePages?.();
    this.unsubscribePages = null;
    for (const unsubscribe of this.pageSceneUnsubs.values()) unsubscribe();
    this.pageSceneUnsubs.clear();
    this.manifestDirty = false;
    this.layerManifestDirty = false;
    this.knownLayerAudiences.clear();
    this.knownLayerVersions.clear();
    this.knownPublicSynced.clear();
    this.pendingHideTombstones.clear();
    this.disposeIdleTicking?.();
    this.disposeIdleTicking = null;
    if (this.outboundTimer !== null) clearTimeout(this.outboundTimer);
    this.outboundTimer = null;
    if (this.snapshotTimer !== null) clearInterval(this.snapshotTimer);
    this.snapshotTimer = null;
    this.syncedVersions.clear();
    this.syncedComments.clear();
    this.presence.clear();
    this.presenceBroadcaster.reset();
    this.roomKey = null;
    this.roomId = null;
    this.role = "editor";
    this.followedPeerId = null;
    this.syncPrivateAddOnly();
    this.setStatus("disconnected");
  }

  get privateMode(): boolean {
    return this.privateModeEnabled;
  }

  setPrivateMode(enabled: boolean): void {
    const wasEnabled = this.privateModeEnabled;
    this.privateModeEnabled = enabled;
    this.syncPrivateAddOnly();
    if (!enabled && (wasEnabled || this.hasAnyHeldElements()) && this.role === "admin") {
      void this.releaseHeldElementsAndSync();
    }
  }

  private allActiveScenes(): Scene[] {
    if (!this.pages) return [this.scene];
    const scenes: Scene[] = [];
    for (const pageId of this.pages.listPageIds()) {
      const scene = this.pages.getScene(pageId);
      if (scene) scenes.push(scene);
    }
    return scenes;
  }

  private hasAnyHeldElements(): boolean {
    return this.allActiveScenes().some((scene) => scene.hasHeldElements());
  }

  private syncPrivateAddOnly(): void {
    const addOnly = !this.tornDown && this.privateModeEnabled && this.role !== "admin";
    for (const scene of this.allActiveScenes()) {
      scene.setPrivateAddOnly(addOnly);
    }
  }

  private async releaseHeldElementsAndSync(): Promise<void> {
    let anyReleased = false;
    if (this.pages) {
      for (const pageId of this.pages.listPageIds()) {
        const scene = this.pages.getScene(pageId);
        if (!scene) continue;
        for (const id of scene.releaseHeldElements()) {
          this.syncedVersions.delete(`${pageId}/${id}`);
          anyReleased = true;
        }
      }
    } else {
      for (const id of this.scene.releaseHeldElements()) {
        this.syncedVersions.delete(id);
        anyReleased = true;
      }
    }
    if (!anyReleased) return;
    if (this.outboundTimer !== null) {
      clearTimeout(this.outboundTimer);
      this.outboundTimer = null;
    }
    await this.flushOutboundNow();
    await this.sendSnapshot();
  }

  /** Publishes the local user's live cursor position (scene coordinates); `null` when the pointer leaves the canvas. Throttled internally — safe to call on every `pointermove`. */
  updateCursor(point: { x: number; y: number } | null): void {
    this.presenceBroadcaster.updateCursor(point);
  }

  /** Updates the locally-tracked selection that rides along with the next presence broadcast. */
  setLocalSelection(selectedElementIds: string[]): void {
    this.presenceBroadcaster.setLocalSelection(selectedElementIds);
  }

  setLocalViewport(viewport: PresenceViewport | null): void {
    this.presenceBroadcaster.setLocalViewport(viewport);
  }

  /** Sends a one-shot emoji reaction to every peer. Lossy and unretried — see `PresenceBroadcaster.sendReaction`. */
  sendReaction(emoji: string): void {
    this.presenceBroadcaster.sendReaction(emoji);
  }

  /** Raises or lowers the local user's hand for every peer to see. */
  setHandRaised(raised: boolean): void {
    this.presenceBroadcaster.setHandRaised(raised);
  }

  /** Follow-mode: `peerId` (or `null` to stop) whose viewport `getFollowedViewport` reports — the caller applies it to the local camera on presence updates. */
  follow(peerId: string | null): void {
    this.followedPeerId = peerId;
  }

  get currentFollowedPeerId(): string | null {
    return this.followedPeerId;
  }
  getFollowedViewport(): PresenceViewport | null {
    if (!this.followedPeerId) return null;
    return this.presence.get(this.followedPeerId)?.viewport ?? null;
  }

  private async connectToRoom(apiBaseUrl: string, roomId: string, keyBase64Url: string, token?: string | null): Promise<void> {
    this.disconnect();
    this.tornDown = false;
    this.roomId = roomId;
    this.roomKey = await importRoomKey(keyBase64Url);
    // The link's own token decides the local role. A link with no token is an editor — that is what
    // every room link created before roles existed looks like, and the relay agrees.
    this.role = roleClaimedByToken(token);
    this.setStatus("connecting");

    this.connection = new ConnectionManager({
      url: buildRoomWebSocketUrl(apiBaseUrl, roomId, token),
      createSocket: this.createSocket,
      onOpen: () => {
        this.setStatus("connected");
        this.connection?.send(JSON.stringify({ type: "snapshot-request" }));
        this.scheduleOutboundSync(); // publish whatever this client already has once the channel opens
      },
      onReconnect: () => {
        // See the module doc: resync presence after an unexpected drop.
        this.presence.clear();
        this.presenceBroadcaster.republish();
      },
      onClose: () => {
        if (!this.tornDown) this.setStatus("connecting");
      },
      // The room is not coming back: the host stopped, this client was refused, or the attempts ran
      // out. Reported as a real disconnect rather than a permanent "connecting", so the UI can offer
      // the session as over instead of showing a spinner with nothing behind it.
      onGiveUp: (reason) => {
        if (this.tornDown) return;
        this.presence.clear();
        this.setStatus("disconnected");
        this.onEnded?.(reason);
      },
      onMessage: (data) => void this.dispatchInbound(data),
      initialBackoffMs: this.initialBackoffMs,
      maxBackoffMs: this.maxBackoffMs,
    });
    this.connection.connect();

    if (this.pages) {
      // One subscription per page's scene (kept current as pages come and go) plus the page-list
      // itself: any page operation dirties the manifest, which the next outbound flush publishes as
      // a prompt snapshot — so a rename/add/delete reaches peers in ~one debounce, not the 30s timer.
      this.refreshPageSubscriptions();
      this.unsubscribePages = this.pages.subscribe(() => {
        this.manifestDirty = true;
        this.refreshPageSubscriptions();
        this.scheduleOutboundSync();
      });
    } else {
      this.recordLayerAudiences(this.scene);
      this.unsubscribeScene = this.scene.subscribe(() => this.onSceneChanged(this.scene));
    }
    this.disposeIdleTicking = this.presence.startIdleTicking(PRESENCE_IDLE_TICK_MS);
    this.snapshotTimer = setInterval(() => void this.sendSnapshot(), SNAPSHOT_INTERVAL_MS);
    this.syncPrivateAddOnly();
  }

  private dispatchInbound(raw: string): Promise<void> {
    if (!this.roomKey) return Promise.resolve();
    const isAdmin = this.role === "admin";
    return handleInboundMessage(raw, {
      // In a pages session, legacy un-tagged traffic lands on the current first page (not a scene
      // captured at connect time, which a page deletion could orphan).
      scene: this.defaultScene(),
      pages: this.pages,
      presence: this.presence,
      roomKey: this.roomKey,
      isAdmin,
      markSynced: (id, version, pageId) => {
        const key = pageId === undefined ? id : `${pageId}/${id}`;
        this.syncedVersions.set(key, version);
        const scene = pageId === undefined ? this.defaultScene() : (this.pages?.getScene(pageId) ?? this.defaultScene());
        const element = scene.getElement(id);
        if (element && !scene.isElementHeld(id) && scene.resolveLayer(element).audience !== "admins") {
          this.knownPublicSynced.add(key);
        }
      },
      markCommentSynced: (id, version, pageId) => this.syncedComments.set(pageId === undefined ? id : `${pageId}/${id}`, version),
      onPeerLeft: (peerId) => this.presence.removePeer(peerId),
      // Republish, don't clear: the newcomer needs to see this client, and every peer already here is
      // still valid. Throttled inside the broadcaster, so a burst of joins costs one send.
      onPeerJoined: () => this.presenceBroadcaster.republish(),
      onSnapshotRequested: () => void this.sendSnapshot(),
      onElementRefused: isAdmin
        ? (targetScene, refusedElement, pageId) => this.handleRefusedElement(targetScene, refusedElement, pageId)
        : undefined,
    });
  }

  private handleRefusedElement(targetScene: Scene, refusedElement: AnyElement, pageId?: string): void {
    const local = targetScene.getElement(refusedElement.id);
    if (!local) {
      // The admin never had this element (a new stroke on a locked layer). Tombstone it so the
      // author and every other peer drop it, instead of leaving it on the screens that accepted
      // the delta before the lock was known.
      targetScene.applyRemoteElement({
        ...refusedElement,
        isDeleted: true,
        version: refusedElement.version + 1,
        versionNonce: randomVersionNonce(),
        updated: Date.now(),
      });
    } else if (remoteElementWins(local, refusedElement)) {
      if (refusedElement.version === local.version) {
        targetScene.updateElement(local.id, {});
      } else {
        targetScene.applyRemoteElement({
          ...local,
          version: refusedElement.version + 1,
          versionNonce: randomVersionNonce(),
          updated: Date.now(),
        });
      }
    }
    const id = local?.id ?? refusedElement.id;
    const syncKey = pageId === undefined ? id : `${pageId}/${id}`;
    this.syncedVersions.delete(syncKey);
    this.scheduleOutboundSync();
  }

  private recordLayerAudiences(scene: Scene, pageId?: string): void {
    const scopeKey = pageId ?? "__single__";
    this.knownLayerAudiences.set(scopeKey, new Map(scene.getLayers().map((layer) => [layer.id, layer.audience])));
    this.knownLayerVersions.set(scopeKey, scene.getLayersVersion());
  }

  private onSceneChanged(scene: Scene, pageId?: string): void {
    if (this.role === "admin") {
      const scopeKey = pageId ?? "__single__";
      const prevMap = this.knownLayerAudiences.get(scopeKey);
      const prevVersion = this.knownLayerVersions.get(scopeKey);
      const nextLayers = scene.getLayers();
      const nextVersion = scene.getLayersVersion();
      if (prevMap) {
        const newlyHidden = new Set<string>();
        const newlyRevealed = new Set<string>();
        for (const layer of nextLayers) {
          const prev = prevMap.get(layer.id);
          if (prev === undefined && layer.audience === "admins") newlyHidden.add(layer.id);
          else if (prev === "admins" && layer.audience === undefined) newlyRevealed.add(layer.id);
        }
        this.recordLayerAudiences(scene, pageId);
        if (newlyHidden.size > 0 || newlyRevealed.size > 0) {
          this.layerManifestDirty = true;
          scene.batch(() => {
            for (const element of [...scene.elementsUnsorted()]) {
              if (element.isDeleted) continue;
              const layerId = scene.resolveLayer(element).id;
              if (newlyHidden.has(layerId)) {
                const updated = scene.updateElement(element.id, {});
                if (updated) {
                  const syncKey = pageId === undefined ? element.id : `${pageId}/${element.id}`;
                  this.pendingHideTombstones.add(syncKey);
                }
              } else if (newlyRevealed.has(layerId)) {
                scene.updateElement(element.id, {});
              }
            }
          });
        } else if (prevVersion !== undefined && nextVersion !== prevVersion) {
          this.layerManifestDirty = true;
        }
      } else {
        this.recordLayerAudiences(scene, pageId);
      }
    }
    this.scheduleOutboundSync();
  }

  private setStatus(status: CollabConnectionStatus): void {
    if (this.status === status) return;
    this.status = status;
    this.onStatusChange?.(status);
  }

  private scheduleOutboundSync(): void {
    if (this.outboundTimer !== null) return;
    this.outboundTimer = setTimeout(() => {
      this.outboundTimer = null;
      void this.flushOutboundNow();
    }, OUTBOUND_SYNC_DEBOUNCE_MS);
  }

  private async flushOutboundNow(): Promise<void> {
    const live = this.liveConnection();
    if (!live || !live.connection.isOpen) return;
    const send = (data: string) => live.connection.send(data);
    // A viewer's element deltas would be dropped by the relay anyway — this guard is politeness, not
    // the boundary (see `apps/collab-server`'s `room-connection-registry.ts` for the real one). It
    // spares the socket a stream of frames that can only be discarded. Comments are NOT guarded: a
    // viewer commenting is the point of the role.
    const mayEdit = this.role !== "viewer";
    const isAdmin = this.role === "admin";
    const promises: Promise<void>[] = [];
    if (this.pages) {
      for (const pageId of this.pages.listPageIds()) {
        const scene = this.pages.getScene(pageId);
        if (scene) {
          if (mayEdit) {
            promises.push(
              flushElementDeltas(
                {
                  scene,
                  pageId,
                  isAdmin,
                  knownPublicSynced: this.knownPublicSynced,
                  pendingHideTombstones: this.pendingHideTombstones,
                  roomKey: live.roomKey,
                  send,
                },
                this.syncedVersions,
              ),
            );
          }
          promises.push(flushCommentDeltas({ scene, pageId, isAdmin, roomKey: live.roomKey, send }, this.syncedComments));
        }
      }
      await Promise.all(promises);
      if (this.manifestDirty || this.layerManifestDirty) {
        this.manifestDirty = false;
        this.layerManifestDirty = false;
        await this.sendSnapshot();
      }
      return;
    }
    if (mayEdit) {
      promises.push(
        flushElementDeltas(
          {
            scene: this.scene,
            isAdmin,
            knownPublicSynced: this.knownPublicSynced,
            pendingHideTombstones: this.pendingHideTombstones,
            roomKey: live.roomKey,
            send,
          },
          this.syncedVersions,
        ),
      );
    }
    promises.push(flushCommentDeltas({ scene: this.scene, isAdmin, roomKey: live.roomKey, send }, this.syncedComments));
    await Promise.all(promises);
    if (this.layerManifestDirty) {
      this.layerManifestDirty = false;
      await this.sendSnapshot();
    }
  }

  /** The scene legacy (un-tagged) inbound traffic applies to — the live first page in a pages session, the fixed scene otherwise. */
  private defaultScene(): Scene {
    if (this.pages) {
      const firstId = this.pages.getManifest().pages[0]?.id;
      const first = firstId !== undefined ? this.pages.getScene(firstId) : null;
      if (first) return first;
    }
    return this.scene;
  }

  /** Reconciles the per-page scene subscriptions with the adapter's current page list. */
  private refreshPageSubscriptions(): void {
    if (!this.pages) return;
    const currentIds = new Set(this.pages.listPageIds());
    for (const [pageId, unsubscribe] of this.pageSceneUnsubs) {
      if (!currentIds.has(pageId)) {
        unsubscribe();
        this.pageSceneUnsubs.delete(pageId);
      }
    }
    for (const pageId of currentIds) {
      if (this.pageSceneUnsubs.has(pageId)) continue;
      const scene = this.pages.getScene(pageId);
      if (scene) {
        this.recordLayerAudiences(scene, pageId);
        this.pageSceneUnsubs.set(pageId, scene.subscribe(() => this.onSceneChanged(scene, pageId)));
      }
    }
    this.syncPrivateAddOnly();
  }

  /** Which page the local user is viewing — rides with presence so peers keep foreign-page cursors off their canvas. */
  setLocalPage(pageId: string | null): void {
    this.presenceBroadcaster.setLocalPage(pageId);
  }

  private async sendSnapshot(): Promise<void> {
    // A snapshot is the whole document — the single most destructive thing a viewer could publish, and
    // the relay drops it. Not sending one also means a viewer never volunteers to answer another peer's
    // `snapshot-request`, leaving that to an editor who actually has authority over the document.
    if (this.role === "viewer") return;
    const live = this.liveConnection();
    if (!live || !live.connection.isOpen) return;
    const send = (data: string) => live.connection.send(data);
    const isAdmin = this.role === "admin";
    if (this.pages) {
      await sendDocumentSnapshot({ isAdmin, roomKey: live.roomKey, send }, this.pages);
      return;
    }
    await sendFullSnapshot({ scene: this.scene, isAdmin, roomKey: live.roomKey, send });
  }
}

/**
 * Asks the relay for a room and its two role tokens (`POST /room`).
 *
 * Kept a plain module function rather than a method: it needs nothing from the session's state, and
 * this way the one network call `startSession` makes outside the WebSocket is trivially swappable via
 * `CollabSessionOptions.createRoom` for tests that run against a socket-only fake relay.
 */
async function requestRoom(apiBaseUrl: string): Promise<MintedRoom> {
  const response = await fetch(`${apiBaseUrl.replace(/\/+$/, "")}/room`, { method: "POST" });
  if (!response.ok) throw new Error(`collab-session: room creation failed (${response.status})`);
  const body: unknown = await response.json();
  if (!isMintedRoom(body)) throw new Error("collab-session: room creation returned an unexpected response");
  return body;
}

/** The relay is not a trusted source of well-formed JSON any more than a peer is — a malformed reply becomes a clean error rather than an `undefined` room id in a URL. */
function isMintedRoom(value: unknown): value is MintedRoom {
  if (typeof value !== "object" || value === null) return false;
  const { roomId, editorToken, viewerToken } = value as Record<string, unknown>;
  return typeof roomId === "string" && roomId !== "" && typeof editorToken === "string" && typeof viewerToken === "string";
}

