import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { Pane } from "./Pane";

describe("Pane", () => {
  it("renders header title and body content", () => {
    render(
      <Pane title="Recent filings">
        <p>row content</p>
      </Pane>,
    );
    expect(screen.getByRole("heading", { name: /recent filings/i })).toBeInTheDocument();
    expect(screen.getByText("row content")).toBeInTheDocument();
  });

  it("when only onExpand is set, body clicks do NOT drill — only the Open button does", async () => {
    const onExpand = vi.fn();
    render(
      <Pane title="Filings" onExpand={onExpand}>
        <p>body</p>
      </Pane>,
    );
    await userEvent.click(screen.getByText("body"));
    expect(onExpand).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: /open/i }));
    expect(onExpand).toHaveBeenCalledOnce();
  });

  it("when onCardClick is set, body clicks invoke it (whole-card drill)", async () => {
    const onCardClick = vi.fn();
    render(
      <Pane title="Price chart" onCardClick={onCardClick}>
        <p>body</p>
      </Pane>,
    );
    await userEvent.click(screen.getByText("body"));
    expect(onCardClick).toHaveBeenCalledOnce();
  });

  it("clicking the Open button stops propagation so onCardClick does not also fire", async () => {
    const onExpand = vi.fn();
    const onCardClick = vi.fn();
    render(
      <Pane title="Price chart" onExpand={onExpand} onCardClick={onCardClick}>
        <p>body</p>
      </Pane>,
    );
    await userEvent.click(screen.getByRole("button", { name: /open/i }));
    expect(onExpand).toHaveBeenCalledOnce();
    expect(onCardClick).not.toHaveBeenCalled();
  });

  it("fillHeight=true applies h-full + flex-col so the pane stretches to the parent grid cell (#647)", () => {
    render(
      <Pane title="Price chart" fillHeight>
        <p>body</p>
      </Pane>,
    );
    const article = document.querySelector("article");
    expect(article?.className).toContain("h-full");
    expect(article?.className).toContain("flex");
    expect(article?.className).toContain("flex-col");
    const body = screen.getByText("body").parentElement;
    expect(body?.className).toContain("flex-1");
    expect(body?.className).toContain("min-h-0");
  });

  it("fillHeight defaults to false — existing panes unchanged", () => {
    render(
      <Pane title="Recent filings">
        <p>body</p>
      </Pane>,
    );
    const article = document.querySelector("article");
    expect(article?.className).not.toContain("h-full");
    const body = screen.getByText("body").parentElement;
    expect(body?.className).not.toContain("flex-1");
  });

  it("article className builder produces no double spaces or trailing space when optional segments are omitted", () => {
    render(
      <Pane title="Recent filings">
        <p>body</p>
      </Pane>,
    );
    const article = document.querySelector("article");
    expect(article?.className).not.toMatch(/ {2,}/);
    expect(article?.className).not.toMatch(/ $/);
  });

  it("clickable card does NOT take role=button (avoids nesting interactive descendants)", () => {
    const onCardClick = vi.fn();
    render(
      <Pane title="Price chart" onCardClick={onCardClick} onExpand={vi.fn()}>
        <button type="button">inner</button>
      </Pane>,
    );
    // Only the descendant Open button + inner button are role=button —
    // the article itself stays a plain article so assistive tech does
    // not flatten the inner controls.
    const buttons = screen.getAllByRole("button");
    expect(buttons.every((b) => b.tagName === "BUTTON")).toBe(true);
    // Clickable affordance is signalled via a data-attribute hook for
    // styling/tests — without role=button, the article stays a
    // semantic article element.
    const article = document.querySelector("article");
    expect(article?.getAttribute("data-clickable")).toBe("true");
  });
});
