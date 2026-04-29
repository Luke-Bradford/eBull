import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Term } from "@/components/Term";

describe("Term", () => {
  it("renders the term as an <abbr> with a native title attr for fallback tooltips", () => {
    render(<Term term="CIK" />);
    const el = screen.getByTestId("term-CIK");
    expect(el.tagName.toLowerCase()).toBe("abbr");
    expect(el.getAttribute("title")).toContain("SEC entity ID");
  });

  it("opens the rich tooltip popover on hover and closes on mouseleave", () => {
    render(<Term term="ROE" />);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    fireEvent.mouseEnter(screen.getByTestId("term-ROE"));
    const tip = screen.getByRole("tooltip");
    expect(tip).toHaveTextContent(/Return on equity/i);
    expect(tip).toHaveTextContent(/Why it matters:/i);
    fireEvent.mouseLeave(screen.getByTestId("term-ROE"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("opens on keyboard focus so screen readers + tab navigation work", () => {
    render(<Term term="P/E ratio" />);
    fireEvent.focus(screen.getByTestId("term-P/E ratio"));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.blur(screen.getByTestId("term-P/E ratio"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("renders the children override as the visible label, looking up under the term key", () => {
    render(<Term term="P/E ratio">P/E</Term>);
    const el = screen.getByTestId("term-P/E ratio");
    expect(el).toHaveTextContent("P/E");
    expect(el).not.toHaveTextContent("P/E ratio");
  });

  it("falls back to plain text when the term isn't in the glossary", () => {
    render(<Term term="UNKNOWN_TERM">my label</Term>);
    expect(screen.queryByTestId("term-UNKNOWN_TERM")).not.toBeInTheDocument();
    // Visible text still renders so the operator sees something.
    expect(screen.getByText("my label")).toBeInTheDocument();
  });
});
