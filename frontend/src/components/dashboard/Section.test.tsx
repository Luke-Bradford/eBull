import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Section } from "./Section";

describe("Section", () => {
  it("renders the title and children", () => {
    render(
      <Section title="Candidates">
        <p>body content</p>
      </Section>,
    );
    expect(screen.getByText("Candidates")).toBeInTheDocument();
    expect(screen.getByText("body content")).toBeInTheDocument();
  });

  // #1858 — the contained-scroll body MUST establish a positioning context.
  // Without `relative`, a `position:absolute` descendant (e.g. a Tailwind
  // `sr-only` label deep in a tall table) has no positioned ancestor, so its
  // containing block resolves to the initial containing block (the viewport)
  // and it ESCAPES the body's `overflow-auto` clipping — extending
  // documentElement.scrollHeight past the viewport and producing dead scroll
  // space below the page. jsdom can't measure layout, so we pin the CSS
  // contract that prevents the escape.
  it("scrollable body is a relative-positioned overflow-auto scroll container (#1858)", () => {
    render(
      <Section title="Candidates" scrollable>
        <p>row</p>
      </Section>,
    );
    const body = screen.getByText("row").parentElement;
    expect(body).not.toBeNull();
    const cls = body!.className;
    expect(cls).toContain("relative");
    expect(cls).toContain("overflow-auto");
    expect(cls).toContain("min-h-0");
    expect(cls).toContain("flex-1");
  });

  it("non-scrollable body does not claim flex space or scroll", () => {
    render(
      <Section title="Static">
        <p>row</p>
      </Section>,
    );
    const body = screen.getByText("row").parentElement;
    expect(body!.className).not.toContain("overflow-auto");
    expect(body!.className).not.toContain("flex-1");
  });
});
