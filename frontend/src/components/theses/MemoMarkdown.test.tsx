import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MemoMarkdown, parseMemoBlocks } from "./MemoMarkdown";

describe("parseMemoBlocks", () => {
  it("splits headings, paragraphs and lists", () => {
    const memo = "## Quality\nStrong moat.\nGrowing share.\n\n- risk one\n- risk two\n\nFinal note.";
    expect(parseMemoBlocks(memo)).toEqual([
      { kind: "heading", text: "Quality" },
      { kind: "para", text: "Strong moat. Growing share." },
      { kind: "list", items: ["risk one", "risk two"] },
      { kind: "para", text: "Final note." },
    ]);
  });

  it("heading terminates a paragraph without a blank line", () => {
    expect(parseMemoBlocks("intro line\n### Risks\nbody")).toEqual([
      { kind: "para", text: "intro line" },
      { kind: "heading", text: "Risks" },
      { kind: "para", text: "body" },
    ]);
  });

  it("supports * bullets and empty input", () => {
    expect(parseMemoBlocks("* a\n* b")).toEqual([{ kind: "list", items: ["a", "b"] }]);
    expect(parseMemoBlocks("")).toEqual([]);
  });

  it("tolerates #### headings (writer contract is #-###; level 4 renders as heading, not literal)", () => {
    expect(parseMemoBlocks("#### Deep dive")).toEqual([{ kind: "heading", text: "Deep dive" }]);
  });
});

describe("MemoMarkdown", () => {
  it("renders bold inline without literal asterisks", () => {
    render(<MemoMarkdown memo="This is **key** context." />);
    expect(screen.getByText("key")).toBeInTheDocument();
    expect(screen.queryByText(/\*\*key\*\*/)).not.toBeInTheDocument();
  });
});
