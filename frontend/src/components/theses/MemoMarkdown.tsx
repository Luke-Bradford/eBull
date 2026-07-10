/**
 * Minimal renderer for the thesis writer's prompt-constrained markdown
 * subset (#2000): #/##/### headings, `- `/`* ` bullet lists, **bold**
 * inline, blank-line paragraphs. The parser deliberately tolerates ####
 * (one level beyond the prompt contract) — a model that over-nests should
 * render a heading, not a literal "####" line. Deliberately in-house —
 * the memo format is our own prompt contract, not arbitrary markdown, so
 * a dependency (and its sanitisation surface) buys nothing. Everything
 * renders as text nodes; no HTML injection path exists.
 */

const BOLD_SPLIT = /(\*\*[^*]+\*\*)/g;

function renderInline(text: string): (string | JSX.Element)[] {
  return text.split(BOLD_SPLIT).map((part, i) =>
    part.startsWith("**") && part.endsWith("**") && part.length > 4 ? (
      <strong key={i} className="font-semibold text-slate-800 dark:text-slate-200">
        {part.slice(2, -2)}
      </strong>
    ) : (
      part
    ),
  );
}

type Block =
  | { kind: "heading"; text: string }
  | { kind: "list"; items: string[] }
  | { kind: "para"; text: string };

export function parseMemoBlocks(memo: string): Block[] {
  const blocks: Block[] = [];
  let list: string[] | null = null;
  let para: string[] = [];

  const flushPara = () => {
    if (para.length > 0) {
      blocks.push({ kind: "para", text: para.join(" ") });
      para = [];
    }
  };
  const flushList = () => {
    if (list !== null) {
      blocks.push({ kind: "list", items: list });
      list = null;
    }
  };

  for (const raw of memo.split("\n")) {
    const line = raw.trim();
    const headingText = /^#{1,4}\s+(.*)$/.exec(line)?.[1];
    const bulletText = /^[-*]\s+(.*)$/.exec(line)?.[1];
    if (line === "") {
      flushPara();
      flushList();
    } else if (headingText !== undefined) {
      flushPara();
      flushList();
      blocks.push({ kind: "heading", text: headingText });
    } else if (bulletText !== undefined) {
      flushPara();
      list = list ?? [];
      list.push(bulletText);
    } else {
      flushList();
      para.push(line);
    }
  }
  flushPara();
  flushList();
  return blocks;
}

export function MemoMarkdown({ memo }: { readonly memo: string }): JSX.Element {
  const blocks = parseMemoBlocks(memo);
  return (
    <div className="max-w-prose space-y-2 text-sm text-slate-700 dark:text-slate-300">
      {blocks.map((block, i) => {
        if (block.kind === "heading") {
          return (
            <h4
              key={i}
              className="pt-1 text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400"
            >
              {block.text}
            </h4>
          );
        }
        if (block.kind === "list") {
          return (
            <ul key={i} className="list-inside list-disc space-y-0.5">
              {block.items.map((item, j) => (
                <li key={j}>{renderInline(item)}</li>
              ))}
            </ul>
          );
        }
        return <p key={i}>{renderInline(block.text)}</p>;
      })}
    </div>
  );
}
