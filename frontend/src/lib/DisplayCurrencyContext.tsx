import { createContext, useContext, type ReactNode } from "react";
import { useAsync } from "@/lib/useAsync";
import { fetchConfig } from "@/api/config";

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
  const { data } = useAsync(() => fetchConfig(), []);
  const displayCurrency = data?.runtime?.display_currency ?? "GBP";
  return (
    <DisplayCurrencyContext.Provider value={{ displayCurrency }}>
      {children}
    </DisplayCurrencyContext.Provider>
  );
}
