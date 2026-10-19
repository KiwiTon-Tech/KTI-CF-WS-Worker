import { describe, expect, it } from "vitest";
import {
  MAX_SYMBOLS_PER_CLIENT,
  planSubscriptions,
} from "../src/subscriptions";

describe("subscription planning", () => {
  it("normalizes, deduplicates, and acknowledges valid symbols", () => {
    expect(planSubscriptions(new Set(), [" aapl ", "AAPL", "btc/usd"])).toEqual({
      accepted: ["AAPL", "BTC/USD"],
      rejected: [],
    });
  });

  it("rejects invalid symbols", () => {
    expect(planSubscriptions(new Set(), ["../../secret", ""])).toEqual({
      accepted: [],
      rejected: [
        { symbol: "../../SECRET", reason: "invalid_symbol" },
        { symbol: "", reason: "invalid_symbol" },
      ],
    });
  });

  it("enforces the per-client symbol limit", () => {
    const current = new Set(
      Array.from({ length: MAX_SYMBOLS_PER_CLIENT }, (_, index) => `S${index}`),
    );

    expect(planSubscriptions(current, ["AAPL", "S1"])).toEqual({
      accepted: ["S1"],
      rejected: [{ symbol: "AAPL", reason: "subscription_limit" }],
    });
  });
});
