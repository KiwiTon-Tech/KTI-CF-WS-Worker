export const MAX_SYMBOLS_PER_CLIENT = 25;
const SYMBOL_PATTERN = /^[A-Z0-9]{1,12}(?:\/[A-Z0-9]{1,12})?$/;

export interface SubscriptionRejection {
  symbol: string;
  reason: "invalid_symbol" | "subscription_limit";
}

export function planSubscriptions(
  current: ReadonlySet<string>,
  input: unknown[],
): { accepted: string[]; rejected: SubscriptionRejection[] } {
  const requested = Array.from(
    new Set(
      input
        .filter((symbol): symbol is string => typeof symbol === "string")
        .map((symbol) => symbol.trim().toUpperCase()),
    ),
  );
  const accepted: string[] = [];
  const rejected: SubscriptionRejection[] = [];
  let size = current.size;

  for (const symbol of requested) {
    if (!SYMBOL_PATTERN.test(symbol)) {
      rejected.push({ symbol, reason: "invalid_symbol" });
      continue;
    }
    if (!current.has(symbol) && size >= MAX_SYMBOLS_PER_CLIENT) {
      rejected.push({ symbol, reason: "subscription_limit" });
      continue;
    }
    accepted.push(symbol);
    if (!current.has(symbol)) size += 1;
  }

  return { accepted, rejected };
}
