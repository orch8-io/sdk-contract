import { createHmac, timingSafeEqual } from "node:crypto";

/** Engine push/webhook signature: hex(HMAC-SHA256(secret, `${ts}.${body}`)). */
export function sign(secret, timestamp, body) {
  return createHmac("sha256", secret).update(`${timestamp}.`).update(body).digest("hex");
}

export function signatureHeader(secret, timestamp, body) {
  return `sha256=${sign(secret, timestamp, body)}`;
}

/** Reference verifier used by the reference adapter and the vector self-test. */
export function verify({ secret, timestamp, signature, body, now, toleranceSecs = 300 }) {
  if (typeof timestamp !== "string" || !/^-?\d+$/.test(timestamp)) return false;
  if (typeof signature !== "string" || !signature.startsWith("sha256=")) return false;
  const hex = signature.slice("sha256=".length);
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) return false;
  const ts = Number.parseInt(timestamp, 10);
  const current = now ?? Math.floor(Date.now() / 1000);
  if (Math.abs(current - ts) > toleranceSecs) return false;
  const expected = createHmac("sha256", secret).update(`${timestamp}.`).update(body).digest();
  return timingSafeEqual(expected, Buffer.from(hex, "hex"));
}
