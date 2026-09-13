import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { AppShell } from "./AppShell";

// The shell pulls in the sidebar, header, notification bell and two nudge
// banners, all of which fetch. None of that is under test here -- the subject is
// the <main> element's CSS contract -- so they are stubbed to nothing.
vi.mock("@/layout/Sidebar", () => ({ Sidebar: () => <aside data-testid="sidebar" /> }));
vi.mock("@/layout/Header", () => ({ Header: () => <header /> }));
vi.mock("@/components/dashboard/BootstrapNudgeBanner", () => ({ BootstrapNudgeBanner: () => null }));
vi.mock("@/components/dashboard/OpenFigiKeyNudgeBanner", () => ({ OpenFigiKeyNudgeBanner: () => null }));
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>("react-router-dom");
  return { ...actual, Outlet: () => <p>routed content</p> };
});

describe("AppShell", () => {
  // #2982 — the scroll container MUST establish a positioning context, the same
  // rule #1858 pinned one level down on `Section`'s scrollable body.
  //
  // Without `relative`, a `position:absolute` descendant with no nearer
  // positioned ancestor (a Tailwind `sr-only` heading is the usual one --
  // `sr-only` IS `position:absolute`) resolves its containing block to the
  // INITIAL containing block rather than this <main>. It then escapes both this
  // element's `overflow-auto` clip and the shell's `overflow-hidden`, because the
  // shell is not its containing block either. Its static position extends
  // documentElement.scrollHeight and the whole app shell scrolls, sliding the
  // <aside> off-screen -- measured on /strategies at 1600x1000 as docScrollHeight
  // 2542 against a 1000px viewport.
  //
  // jsdom performs no layout, so scrollHeight cannot be measured here and the
  // assertion pins the CSS contract that prevents the escape. The dimensional
  // check belongs to FE-QA; this is the regression guard.
  it("main is a relative-positioned overflow-auto scroll container (#2982)", () => {
    render(
      <MemoryRouter>
        <AppShell />
      </MemoryRouter>,
    );

    const main = screen.getByText("routed content").closest("main");
    expect(main).not.toBeNull();
    expect(main!.className).toContain("relative");
    expect(main!.className).toContain("overflow-auto");
  });

  it("the shell itself still clips and does not scroll", () => {
    render(
      <MemoryRouter>
        <AppShell />
      </MemoryRouter>,
    );

    // The outer shell is the element that must never scroll: `h-screen
    // w-screen overflow-hidden`. If it ever gains `overflow-auto`, the document
    // can scroll again through a different route than #2982's and the assertion
    // above would not see it.
    const shell = screen.getByTestId("sidebar").parentElement;
    expect(shell).not.toBeNull();
    expect(shell!.className).toContain("overflow-hidden");
    expect(shell!.className).toContain("h-screen");
  });
});
