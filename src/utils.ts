import fs from 'node:fs';
import path from 'node:path';

export function toSnakeLower(value: string): string {
  return value.replace(/\s+/g, '_').replace(/-/g, '_').toLowerCase();
}

export function parseList(value?: string): string[] | undefined {
  if (!value) return undefined;
  const items = value
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
  return items.length ? items : undefined;
}

export function quoteIdent(value: string): string {
  return '"' + value.replace(/"/g, '""') + '"';
}

export function configFileExists(filePath: string): boolean {
  try {
    return fs.statSync(filePath).isFile();
  } catch {
    return false;
  }
}

export function resolveConfigPath(inputPath?: string): string {
  if (!inputPath) {
    return path.resolve(process.cwd(), 'seeder.config.json');
  }
  return path.resolve(process.cwd(), inputPath);
}

export function topologicalSort(nodes: string[], edges: Map<string, Set<string>>): string[] {
  const inDegree = new Map<string, number>();
  for (const node of nodes) {
    inDegree.set(node, 0);
  }
  for (const [from, tos] of edges.entries()) {
    if (!inDegree.has(from)) continue;
    for (const to of tos) {
      if (!inDegree.has(to)) continue;
      inDegree.set(to, (inDegree.get(to) ?? 0) + 1);
    }
  }

  const queue: string[] = [];
  for (const [node, degree] of inDegree.entries()) {
    if (degree === 0) queue.push(node);
  }

  const result: string[] = [];
  while (queue.length) {
    const node = queue.shift();
    if (!node) break;
    result.push(node);
    const tos = edges.get(node);
    if (!tos) continue;
    for (const to of tos) {
      if (!inDegree.has(to)) continue;
      const next = (inDegree.get(to) ?? 0) - 1;
      inDegree.set(to, next);
      if (next === 0) queue.push(to);
    }
  }

  if (result.length !== nodes.length) {
    const remaining = nodes.filter((node) => !result.includes(node));
    throw new Error(
      `Cycle detected in table dependencies. Cyclic tables: ${remaining.join(', ')}`
    );
  }

  return result;
}
