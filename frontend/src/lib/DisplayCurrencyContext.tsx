import { createContext, useContext, type ReactNode } from "react";

import { useConfig } from "@/lib/ConfigContext";

interface DisplayCurrencyContextValue {
  displayCurrency: string;
}

const DisplayCurrencyContext = createContext<DisplayCurrencyContextValue>({
  displayCurrency: "GBP",
});

export function useDisplayCurrency(): string {
  return useContext(DisplayCurrencyContext).displayCurrency;
}

export function DisplayCurrencyProvider({ children }: { children: ReactNode }) {
  const { data } = useConfig();
  const displayCurrency = data?.runtime?.display_currency ?? "GBP";
  return (
    <DisplayCurrencyContext.Provider value={{ displayCurrency }}>
      {children}
    </DisplayCurrencyContext.Provider>
  );
}
