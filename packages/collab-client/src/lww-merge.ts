/**
 * Last-writer-wins conflict resolution for remote element deltas — the entire correctness guarantee
 * for concurrent edits, so this must be implemented exactly (not approximated) and identically on
 * every client. The comparison itself now lives in the engine (`scene/lww-record.ts`) because comment
 * records resolve with the identical rule and the engine's own store has to apply it inside
 * `applyRemote*` — one implementation, two callers, rather than two implementations that could drift
 * and split the network. This module keeps the element-specific validation and apply step.
 * Fractional-index z-order
 * conflicts need no separate handling: `index` is just another field on the element, so whichever
 * element wins the version/versionNonce comparison carries its `index` along as part of the same
 * atomic decision, and fractional-index comparison is itself total-order-consistent regardless of
 * application order.
 */
import type { AnyElement, Scene } from "@benny2744/engine";
import { lwwRecordWins } from "@benny2744/engine";
import type { CollabRole } from "./collab-session";

/**
 * Structural validation for an inbound remote element — the decrypted payload is still untrusted at
 * this point (a buggy or actively malicious peer, or ciphertext that happened to decrypt+decompress
 * into *some* valid JSON without being a real element). Every field this module reads for the LWW
 * comparison or hands to `Scene.applyRemoteElement` is checked here first; anything that fails is
 * rejected rather than partially trusted.
 */
export function isPlausibleRemoteElement(value: unknown): value is AnyElement {
  if (typeof value !== "object" || value === null) return false;
  const el = value as Record<string, unknown>;
  return (
    typeof el.id === "string" &&
    el.id.length > 0 &&
    typeof el.type === "string" &&
    el.type.length > 0 &&
    typeof el.version === "number" &&
    Number.isFinite(el.version) &&
    typeof el.versionNonce === "number" &&
    Number.isFinite(el.versionNonce) &&
    typeof el.index === "string" &&
    (el.layerId === undefined || typeof el.layerId === "string") &&
    (el.adminLocked === undefined || el.adminLocked === true) &&
    typeof el.isDeleted === "boolean" &&
    typeof el.updated === "number"
  );
}

/**
 * Role gate for inbound element mutations: non-admins cannot modify an admin-locked element (or an
 * element on an admin-locked layer, or move one onto an admin-locked layer), and cannot add or
 * remove `adminLocked` itself.
 */
export function isElementMutationAllowed(
  scene: Scene,
  local: AnyElement | undefined,
  remote: AnyElement,
  senderRole: CollabRole = "admin",
): boolean {
  if (senderRole === "admin") return true;
  if (Boolean(local?.adminLocked) !== Boolean(remote.adminLocked)) return false;
  if (local?.adminLocked === true) return false;
  const localLayer = local ? scene.resolveLayer(local) : undefined;
  const remoteLayer = scene.resolveLayer(remote);
  if (localLayer && (localLayer.adminLocked === true || localLayer.audience === "admins")) return false;
  if (remoteLayer.adminLocked === true || remoteLayer.audience === "admins") return false;
  return true;
}

/**
 * `true` when `remote` should overwrite `local` (or `local` is absent — a brand-new element) under the
 * LWW rule described in the module doc. `false` for an already-applied identical delta (same version
 * and versionNonce) as well as for a strictly older/losing one — both cases mean "nothing to change",
 * which callers use to skip a redundant `Scene.applyRemoteElement`/rebroadcast.
 */
export function remoteElementWins(local: AnyElement | undefined, remote: AnyElement): boolean {
  return lwwRecordWins(local, remote);
}

/**
 * Validates, role-checks, LWW-compares, and applies a single remote element delta to `scene` — the
 * one call site an inbound-message handler needs, keeping "decide" and "apply" atomic from the
 * caller's perspective so there's no window where a second concurrent inbound delta could see a
 * stale comparison result. Returns whether the delta was actually applied (a caller uses this to
 * avoid rebroadcasting a rejected/no-op/malformed delta back onto the wire).
 */
export function mergeRemoteElement(scene: Scene, remote: unknown, senderRole: CollabRole = "admin"): boolean {
  if (!isPlausibleRemoteElement(remote)) return false;
  const local = scene.getElement(remote.id);
  if (!isElementMutationAllowed(scene, local, remote, senderRole)) return false;
  if (!remoteElementWins(local, remote)) return false;
  scene.applyRemoteElement(remote);
  return true;
}
