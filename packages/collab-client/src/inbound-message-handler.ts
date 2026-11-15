/**
 * Parses and dispatches a single inbound WebSocket frame for `collab-session.ts` — pulled into its own
 * module so the "untrusted wire data in, validated effect on `Scene`/presence out" dispatch logic is
 * unit-testable in isolation from `CollabSession`'s connection/timer lifecycle. Every branch here treats
 * `raw` as fully attacker-controlled: a malformed JSON frame, an unrecognized `type`, ciphertext that
 * fails to decrypt, or a payload shape that doesn't match its `type` are all silently ignored rather
 * than thrown — a single hostile or buggy peer must never crash another peer's session.
 */
import { decryptEnvelope } from "./message-codec";
import type { RoomEnvelope } from "./message-codec";
import { isElementMutationAllowed, isPlausibleRemoteElement, remoteElementWins } from "./lww-merge";
import { isPlausibleManifest } from "./pages-adapter";
import { isPlausibleLayersManifest } from "./layers-adapter";
import type { LayersManifest } from "./layers-adapter";
import { mergeRemoteComments, mergeRemoteCommentMessage, mergeRemoteCommentThread } from "./comments-adapter";
import type { CollabPagesAdapter } from "./pages-adapter";
import type { CollabRole } from "./collab-session";
import type { AnyElement, Scene } from "@benny2744/engine";
import type { PresenceStore } from "./presence-state";

export interface InboundMessageDeps {
  scene: Scene;
  presence: PresenceStore;
  roomKey: CryptoKey;
  /** Multi-page routing: a delta's `pageId` resolves through this adapter (creating unknown pages, refusing deleted ones); absent ⇒ everything applies to `scene`. */
  pages?: CollabPagesAdapter;
  /** Records that element `id` is now synced at `version` — prevents the next outbound scan from redundantly re-sending an element this session just received. `pageId` mirrors the delta's tag so the caller can namespace its record the same way the outbound scan does. */
  markSynced(id: string, version: number, pageId?: string): void;
  onPeerLeft(peerId: string): void;
  /**
   * Fires when the relay announces a new member. Presence is ephemeral and never replayed, so a peer
   * that joins a quiet room sees nobody at all until each existing peer happens to move their mouse —
   * which for follow-mode means the peer list they would follow *from* is empty. Existing peers answer
   * by republishing their own presence once, the same shape `onReconnect` already relies on.
   */
  onPeerJoined(): void;
  /**
   * Fires when the *relay* forwards a `snapshot-request` to this peer — meaning the Durable Object had
   * no stored snapshot yet to answer a newcomer/reconnecting peer directly (see
   * `apps/collab-server/src/room-connection-registry.ts`'s fast-path-vs-broadcast doc) and is asking
   * any already-connected peer to publish one. The handler responds by sending a fresh snapshot
   * immediately rather than waiting for the periodic timer, so a brand-new room member sees existing
   * state promptly instead of only catching whatever element-deltas happen to arrive after they joined.
   */
  onSnapshotRequested(): void;
  /**
   * Records that a comment record is now synced at `version`, in the caller's SEPARATE comment map —
   * see `outbound-sync.ts`'s `flushCommentDeltas` for why it is not the element map.
   *
   * OPTIONAL, unlike `markSynced`: this interface is part of the package's public surface, and a host
   * that predates comments must keep compiling. Omitting it costs only a redundant re-send of a
   * just-received record on the next outbound scan (the receiving peer's LWW then discards it as an
   * already-applied identical delta) — never correctness.
   */
  markCommentSynced?(id: string, version: number, pageId?: string): void;
  /** Whether the receiving client is an admin — tracks held private-mode elements and enforces the add-only rule. */
  isAdmin?: boolean;
  /** Fires when an inbound element delta was refused due to sender role (e.g. non-admin edit to an admin-locked element). */
  onElementRefused?(scene: Scene, element: AnyElement, pageId?: string): void;
}

/**
 * Absent `senderRole` is treated as admin so frames from relays that predate role stamping keep
 * working. A relay that admits untrusted peers must stamp `senderRole` on every broadcast frame.
 * The ModChat relay always does. Forwarding a client frame without that stamp would let the peer
 * act as admin, including publishing layer locks and admin-only snapshots.
 */
function readSenderRole(value: unknown): CollabRole {
  if (value === undefined) return "admin";
  if (value === "admin" || value === "editor" || value === "viewer") return value;
  return "editor";
}

function isLayerManifestAllowed(target: Scene, manifest: LayersManifest, senderRole: CollabRole): boolean {
  if (senderRole === "admin") return true;
  const localById = new Map(target.getLayers().map((layer) => [layer.id, layer]));
  const remoteIds = new Set<string>();
  for (const remoteLayer of manifest.layers) {
    remoteIds.add(remoteLayer.id);
    const localLayer = localById.get(remoteLayer.id);
    if (Boolean(localLayer?.adminLocked) !== Boolean(remoteLayer.adminLocked)) return false;
    if (localLayer?.audience !== remoteLayer.audience) return false;
  }
  for (const localLayer of localById.values()) {
    if ((localLayer.adminLocked || localLayer.audience === "admins") && !remoteIds.has(localLayer.id)) return false;
  }
  return true;
}

function mergeOneElement(
  target: Scene,
  rawElement: unknown,
  deps: InboundMessageDeps,
  senderRole: CollabRole,
  pageId?: string,
  isHeld = false,
): void {
  if (!isPlausibleRemoteElement(rawElement)) return;
  const local = target.getElement(rawElement.id);
  if (isHeld && deps.isAdmin && local !== undefined && !target.isElementHeld(rawElement.id)) {
    deps.onElementRefused?.(target, rawElement, pageId);
    return;
  }
  if (!isElementMutationAllowed(target, local, rawElement, senderRole)) {
    deps.onElementRefused?.(target, rawElement, pageId);
    return;
  }
  if (!remoteElementWins(local, rawElement)) return;
  if (isHeld && deps.isAdmin && local === undefined) {
    target.markElementHeld(rawElement.id);
  }
  target.applyRemoteElement(rawElement);
  deps.markSynced(rawElement.id, rawElement.version, pageId);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** The scene a tagged payload lands in: an untagged one goes to the default `scene` (a pre-pages peer, or a session with no adapter); a tagged one resolves — and may refuse (`null`, deleted page) — through the adapter. */
function resolveTargetScene(pageId: unknown, deps: InboundMessageDeps): Scene | null {
  if (typeof pageId !== "string" || !deps.pages) return deps.scene;
  return deps.pages.ensureScene(pageId);
}

async function applyElementPayload(payload: unknown, deps: InboundMessageDeps, senderRole: CollabRole, isHeld: boolean): Promise<void> {
  if (!isRecord(payload)) return;
  const target = resolveTargetScene(payload.pageId, deps);
  if (!target) return; // a delta racing the deletion of its page must not resurrect it
  const pageId = typeof payload.pageId === "string" ? payload.pageId : undefined;
  mergeOneElement(target, payload.element, deps, senderRole, pageId, isHeld);
}

/** One `comment-delta` frame: exactly one of `thread`/`message`, routed to its page the same way an element delta is. */
function applyCommentPayload(payload: unknown, deps: InboundMessageDeps): void {
  if (!isRecord(payload)) return;
  const target = resolveTargetScene(payload.pageId, deps);
  if (!target) return; // a delta racing the deletion of its page must not resurrect it
  const pageId = typeof payload.pageId === "string" ? payload.pageId : undefined;

  const thread = mergeRemoteCommentThread(target, payload.thread);
  if (thread) deps.markCommentSynced?.(thread.id, thread.version, pageId);

  const message = mergeRemoteCommentMessage(target, payload.message);
  if (message) deps.markCommentSynced?.(message.id, message.version, pageId);
}

function mergeElementsInto(target: Scene, elements: unknown[], deps: InboundMessageDeps, senderRole: CollabRole, pageId?: string): void {
  for (const element of elements) {
    mergeOneElement(target, element, deps, senderRole, pageId);
  }
}

/** A snapshot's comment arrays for one scene — absent on a pre-comments peer's snapshot, which is simply nothing to merge. */
function mergeCommentsInto(target: Scene, rawThreads: unknown, rawMessages: unknown, deps: InboundMessageDeps, pageId?: string): void {
  const applied = mergeRemoteComments(target, rawThreads, rawMessages);
  for (const thread of applied.threads) deps.markCommentSynced?.(thread.id, thread.version, pageId);
  for (const message of applied.messages) deps.markCommentSynced?.(message.id, message.version, pageId);
}

async function applySnapshotPayload(payload: unknown, deps: InboundMessageDeps, senderRole: CollabRole): Promise<void> {
  if (!isRecord(payload)) return;

  // Multi-page snapshot: apply the page-list manifest first (LWW — see `pages-adapter.ts`), then
  // merge each page's elements into whichever page list survived that decision. Element merges run
  // regardless of whether the manifest won: elements are their own per-element LWW.
  if (Array.isArray(payload.pages) && deps.pages) {
    if (isPlausibleManifest(payload.manifest)) deps.pages.applyRemoteManifest(payload.manifest);
    for (const entry of payload.pages) {
      if (!isRecord(entry) || typeof entry.id !== "string" || !Array.isArray(entry.elements)) continue;
      const target = deps.pages.ensureScene(entry.id);
      if (!target) continue;
      // Layers before elements: membership then resolves against the adopted list immediately (and
      // a just-materialized remote page trades its constructor default for the real layer state —
      // the read-time orphan rule reunites any elements that arrived first).
      if (isPlausibleLayersManifest(entry.layers) && isLayerManifestAllowed(target, entry.layers, senderRole)) {
        const adopted = deps.pages.applyRemoteLayersManifest(entry.id, entry.layers);
        // A student who edited their own layer list can outrank the teacher's manifest version.
        // The lock and audience still apply: those two fields are admin policy, not document LWW.
        if (!adopted && senderRole === "admin") target.applyAdminLayerPolicy(entry.layers);
      }
      if (deps.isAdmin && senderRole === "admin" && Array.isArray(entry.heldElementIds)) {
        for (const heldId of entry.heldElementIds) {
          if (typeof heldId === "string" && heldId.length > 0) target.markElementHeld(heldId);
        }
      }
      mergeElementsInto(target, entry.elements, deps, senderRole, entry.id);
      mergeCommentsInto(target, entry.comments, entry.commentMessages, deps, entry.id);
    }
    return;
  }

  // Legacy single-scene snapshot (a pre-pages peer): everything lands in the default scene.
  if (!Array.isArray(payload.elements)) return;
  if (deps.isAdmin && senderRole === "admin" && Array.isArray(payload.heldElementIds)) {
    for (const heldId of payload.heldElementIds) {
      if (typeof heldId === "string" && heldId.length > 0) deps.scene.markElementHeld(heldId);
    }
  }
  mergeElementsInto(deps.scene, payload.elements, deps, senderRole);
  mergeCommentsInto(deps.scene, payload.comments, payload.commentMessages, deps);
}

/** Entry point: parse `raw`, route by `type`, decrypt+apply. Never throws — every failure path is a silent no-op by design (see module doc). */
export async function handleInboundMessage(raw: string, deps: InboundMessageDeps): Promise<void> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return;
  }
  if (!isRecord(parsed) || typeof parsed.type !== "string") return;

  if (parsed.type === "peer-left") {
    if (typeof parsed.peerId === "string") deps.onPeerLeft(parsed.peerId);
    return;
  }
  if (parsed.type === "peer-joined") {
    deps.onPeerJoined();
    return;
  }
  if (parsed.type === "snapshot-request") {
    deps.onSnapshotRequested();
    return;
  }

  const envelope = parsed as unknown as RoomEnvelope & { peerId?: unknown; senderRole?: unknown; held?: unknown };
  const senderRole = readSenderRole(envelope.senderRole);
  const isHeld = envelope.held === true;
  if (envelope.type === "element-delta") {
    const decrypted = await decryptEnvelope(deps.roomKey, envelope);
    if (decrypted.ok) await applyElementPayload(decrypted.payload, deps, senderRole, isHeld);
    return;
  }
  if (envelope.type === "comment-delta") {
    const decrypted = await decryptEnvelope(deps.roomKey, envelope);
    if (decrypted.ok) applyCommentPayload(decrypted.payload, deps);
    return;
  }
  if (envelope.type === "snapshot") {
    const decrypted = await decryptEnvelope(deps.roomKey, envelope);
    if (decrypted.ok) await applySnapshotPayload(decrypted.payload, deps, senderRole);
    return;
  }
  if (envelope.type === "presence" && typeof envelope.peerId === "string") {
    const decrypted = await decryptEnvelope(deps.roomKey, envelope, { compress: false });
    if (decrypted.ok) deps.presence.applyUpdate(envelope.peerId, decrypted.payload);
  }
}
