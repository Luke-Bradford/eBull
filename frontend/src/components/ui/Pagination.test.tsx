import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { Pagination } from "./Pagination";

describe("Pagination", () => {
  it("renders nothing when totalPages <= 1", () => {
    const { container } = render(
      <Pagination page={0} totalPages={1} onPageChange={vi.fn()} />,
    );
    expect(container.innerHTML).toBe("");
  });

  it("renders page numbers for small page count", () => {
    render(<Pagination page={0} totalPages={4} onPageChange={vi.fn()} />);
    expect(screen.getByText("Page 1 of 4")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("4")).toBeInTheDocument();
  });

  it("marks current page with aria-current", () => {
    render(<Pagination page={1} totalPages={4} onPageChange={vi.fn()} />);
    const btn = screen.getByText("2");
    expect(btn).toHaveAttribute("aria-current", "page");
    expect(screen.getByText("1")).not.toHaveAttribute("aria-current");
  });

  it("disables Previous on first page", () => {
    render(<Pagination page={0} totalPages={5} onPageChange={vi.fn()} />);
    expect(screen.getByText("Previous")).toBeDisabled();
    expect(screen.getByText("Next")).not.toBeDisabled();
  });

  it("disables Next on last page", () => {
    render(<Pagination page={4} totalPages={5} onPageChange={vi.fn()} />);
    expect(screen.getByText("Next")).toBeDisabled();
    expect(screen.getByText("Previous")).not.toBeDisabled();
  });

  it("calls onPageChange with correct index on page click", async () => {
    const onChange = vi.fn();
    render(<Pagination page={0} totalPages={5} onPageChange={onChange} />);
    const user = userEvent.setup();
    await user.click(screen.getByText("3"));
    expect(onChange).toHaveBeenCalledWith(2);
  });

  it("calls onPageChange on Previous click", async () => {
    const onChange = vi.fn();
    render(<Pagination page={2} totalPages={5} onPageChange={onChange} />);
    const user = userEvent.setup();
    await user.click(screen.getByText("Previous"));
    expect(onChange).toHaveBeenCalledWith(1);
  });

  it("calls onPageChange on Next click", async () => {
    const onChange = vi.fn();
    render(<Pagination page={2} totalPages={5} onPageChange={onChange} />);
    const user = userEvent.setup();
    await user.click(screen.getByText("Next"));
    expect(onChange).toHaveBeenCalledWith(3);
  });

  it("renders ellipsis for large page counts", () => {
    render(<Pagination page={5} totalPages={20} onPageChange={vi.fn()} />);
    const ellipses = screen.getAllByText("...");
    expect(ellipses.length).toBe(2); // one before, one after
    // First and last page always visible
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.getByText("20")).toBeInTheDocument();
    // Window around current (page 5 = display 6)
    expect(screen.getByText("5")).toBeInTheDocument();
    expect(screen.getByText("6")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
  });

  it("no leading ellipsis when current is near start", () => {
    render(<Pagination page={1} totalPages={20} onPageChange={vi.fn()} />);
    // Pages 1,2,3 visible + last page, one trailing ellipsis
    const ellipses = screen.getAllByText("...");
    expect(ellipses.length).toBe(1);
  });

  it("no trailing ellipsis when current is near end", () => {
    render(<Pagination page={18} totalPages={20} onPageChange={vi.fn()} />);
    const ellipses = screen.getAllByText("...");
    expect(ellipses.length).toBe(1);
  });
});
