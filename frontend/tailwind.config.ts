import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  // Class-based dark mode: the ThemeProvider toggles `<html class="dark">`.
  // Tracking issue #690.
  darkMode: "class",
  theme: {
    extend: {},
  },
  plugins: [],
} satisfies Config;
