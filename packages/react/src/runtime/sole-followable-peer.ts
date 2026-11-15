/**
 * The one peer a classroom viewer can follow, when there is exactly one.
 * A viewer sees only the teacher, because viewer presence is not relayed.
 */
export interface FollowablePeer {
  peerId: string;
  viewport: unknown;
  pageId?: string;
}

export function soleFollowablePeerId(
  peers: ReadonlyArray<FollowablePeer>,
  localPageId: string | null,
): string | null {
  const followable = peers.filter((peer) => {
    if (peer.viewport === null) return false;
    if (peer.pageId === undefined || localPageId === null) return true;
    return peer.pageId === localPageId;
  });
  return followable.length === 1 ? followable[0]!.peerId : null;
}
