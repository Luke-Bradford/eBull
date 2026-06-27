import { describe, expect, it } from "vitest";

import { darkTheme, defaultTooltipStyle, lightTheme } from "@/lib/chartTheme";

describe("defaultTooltipStyle", () => {
  it("themes the surface from the dark palette (the white-card fix)", () => {
    const style = defaultTooltipStyle(darkTheme);
    expect(style.backgroundColor).toBe(darkTheme.bg);
    expect(style.color).toBe(darkTheme.textPrimary);
    expect(style.border).toBe(`1px solid ${darkTheme.borderColor}`);
  });

  it("themes the surface from the light palette", () => {
    const style = defaultTooltipStyle(lightTheme);
    expect(style.backgroundColor).toBe(lightTheme.bg);
    expect(style.color).toBe(lightTheme.textPrimary);
    expect(style.border).toBe(`1px solid ${lightTheme.borderColor}`);
  });

  it("preserves the 11px size and rounded corner across themes", () => {
    for (const theme of [lightTheme, darkTheme]) {
      const style = defaultTooltipStyle(theme);
      expect(style.fontSize).toBe("11px");
      expect(style.borderRadius).toBe(4);
    }
  });

  it("yields distinct surfaces per theme so light and dark never collide", () => {
    expect(defaultTooltipStyle(lightTheme).backgroundColor).not.toBe(
      defaultTooltipStyle(darkTheme).backgroundColor,
    );
  });
});
