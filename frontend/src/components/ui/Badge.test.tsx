import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Badge, type BadgeTone } from "@/components/ui/Badge";

describe("Badge", () => {
  it("carries meaning in the text, so the label survives colour being ignored", () => {
    render(<Badge tone="risk">EXIT</Badge>);
    expect(screen.getByText("EXIT")).toBeInTheDocument();
  });

  it("ships a light AND a dark class for every tone (dark-gate contract)", () => {
    const tones: BadgeTone[] = ["ok", "warn", "risk", "info", "neutral"];
    for (const tone of tones) {
      const { container, unmount } = render(<Badge tone={tone}>{tone}</Badge>);
      const cls = container.querySelector("span")?.className ?? "";
      expect(cls, `${tone} light bg`).toMatch(/(?<!dark:)bg-[a-z]+-\d/);
      expect(cls, `${tone} dark bg`).toContain("dark:bg-");
      expect(cls, `${tone} dark border`).toContain("dark:border-");
      expect(cls, `${tone} dark text`).toContain("dark:text-");
      unmount();
    }
  });

  it("defaults to the neutral tone rather than rendering an unstyled or blank pill", () => {
    const { container } = render(<Badge>unmapped</Badge>);
    expect(container.querySelector("span")?.className).toContain("bg-slate-100");
  });

  it("applies uppercase only when asked, so prose labels keep their casing", () => {
    const { container: plain } = render(<Badge>Strong challenge</Badge>);
    expect(plain.querySelector("span")?.className).not.toContain("uppercase");

    const { container: caps } = render(<Badge uppercase>buy</Badge>);
    expect(caps.querySelector("span")?.className).toContain("uppercase");
  });

  it("passes through title / data-* so call sites keep hover copy and test hooks", () => {
    render(
      <Badge tone="warn" title="FX rate unavailable" data-testid="fx-badge" data-live="false">
        USD
      </Badge>,
    );
    const el = screen.getByTestId("fx-badge");
    expect(el).toHaveAttribute("title", "FX rate unavailable");
    expect(el).toHaveAttribute("data-live", "false");
  });

  it("merges caller className (margins/layout) instead of dropping it", () => {
    const { container } = render(<Badge className="ml-1.5">USD</Badge>);
    const cls = container.querySelector("span")?.className ?? "";
    expect(cls).toContain("ml-1.5");
    expect(cls).toContain("rounded");
  });
});
