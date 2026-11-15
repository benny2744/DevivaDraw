/**
 * Point this fork's published packages at @benny2744.
 * Re-run after merging upstream: it only rewrites the three package names
 * ModChat publishes, and it appends -modchat.1 when that suffix is missing.
 */
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const REPLACEMENTS = [
  ["@deviva-draw/collab-client", "@benny2744/collab-client"],
  ["@deviva-draw/engine", "@benny2744/engine"],
  ["@deviva-draw/react", "@benny2744/react"],
];
const VERSIONED = new Set([
  "packages/engine/package.json",
  "packages/react/package.json",
  "packages/collab-client/package.json",
]);
const SKIP = new Set(["node_modules", "dist", ".git", ".wrangler", "playwright-report", "test-results"]);
const TEXT = new Set([".json", ".ts", ".tsx", ".mjs", ".js", ".md", ".mdx", ".yml", ".yaml", ".html", ".css", ".mts"]);

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    if (SKIP.has(name)) continue;
    const path = join(dir, name);
    const stat = statSync(path);
    if (stat.isDirectory()) walk(path, out);
    else if (stat.isFile() && stat.size < 2_000_000 && TEXT.has(extname(name))) out.push(path);
  }
  return out;
}

let changed = 0;
for (const path of walk(ROOT)) {
  if (path.endsWith("apply-fork-names.mjs")) continue;
  const relativePath = relative(ROOT, path).replaceAll("\\", "/");
  let text = readFileSync(path, "utf8");
  const original = text;
  for (const [from, to] of REPLACEMENTS) text = text.replaceAll(from, to);
  if (VERSIONED.has(relativePath)) {
    text = text.replaceAll("https://github.com/nhtera/DevivaDraw", "https://github.com/benny2744/DevivaDraw");
    text = text.replace(/"version": "([^"]+)"/, (_match, version) => {
      const next = version.includes("-modchat.") ? version : `${version}-modchat.1`;
      return `"version": "${next}"`;
    });
  }
  if (text !== original) {
    writeFileSync(path, text);
    changed += 1;
  }
}
console.log(`updated ${changed} files`);
