/**
 * apiFetch — 202 body handling (#1064 PR2).
 *
 * Pre-PR2 the wrapper short-circuited every 202 to ``undefined``; the
 * comment claimed empty body for all 202 callers. PR1b-2 made
 * ``POST /jobs/{name}/run`` return ``{request_id: N}`` on 202 so the
 * Advanced disclosure (PR2) can pivot the operator to the queue row.
 * This test pins the new contract: read JSON when present, fall back
 * to ``undefined`` on empty body / unparseable text.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { apiFetch } from "@/api/client";

const originalFetch = globalThis.fetch;

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function emptyResponse(status: number): Response {
  return new Response(null, { status });
}

beforeEach(() => {
  globalThis.fetch = vi.fn();
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("apiFetch — 202 body handling", () => {
  it("parses JSON body on 202 when present", async () => {
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(
      jsonResponse(202, { request_id: 42 }),
    );
    const result = await apiFetch<{ request_id: number }>("/jobs/x/run", {
      method: "POST",
    });
    expect(result).toEqual({ request_id: 42 });
  });

  it("returns undefined on 202 with empty body (legacy contract)", async () => {
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(emptyResponse(202));
    const result = await apiFetch<undefined>("/legacy/202", { method: "POST" });
    expect(result).toBeUndefined();
  });

  it("returns undefined on 202 when body is non-JSON text", async () => {
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(
      new Response("plain text", { status: 202 }),
    );
    const result = await apiFetch<undefined>("/legacy/202", { method: "POST" });
    expect(result).toBeUndefined();
  });

  it("204 still returns undefined unconditionally", async () => {
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(emptyResponse(204));
    const result = await apiFetch<undefined>("/auth/logout", {
      method: "POST",
    });
    expect(result).toBeUndefined();
  });
});
