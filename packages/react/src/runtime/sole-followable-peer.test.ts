import { describe, expect, it } from "vitest";
import { soleFollowablePeerId } from "./sole-followable-peer";

const teacher = { peerId: "teacher", viewport: { x: 1, y: 1, zoom: 1 }, pageId: "page-1" };

describe("soleFollowablePeerId", () => {
  it("follows the only peer that has a viewport", () => {
    expect(soleFollowablePeerId([teacher], "page-1")).toBe("teacher");
  });

  it("ignores a peer that has not published a viewport", () => {
    expect(soleFollowablePeerId([{ ...teacher, viewport: null }], "page-1")).toBeNull();
  });

  it("does not guess when two peers could be followed", () => {
    const other = { ...teacher, peerId: "other" };
    expect(soleFollowablePeerId([teacher, other], "page-1")).toBeNull();
  });

  it("stays on the local page when both sides name a page", () => {
    expect(soleFollowablePeerId([teacher], "page-2")).toBeNull();
  });

  it("follows a peer that has not named a page", () => {
    expect(soleFollowablePeerId([{ peerId: "teacher", viewport: teacher.viewport }], "page-1")).toBe("teacher");
  });

  it("follows when this client has not named a page yet", () => {
    expect(soleFollowablePeerId([teacher], null)).toBe("teacher");
  });
});
