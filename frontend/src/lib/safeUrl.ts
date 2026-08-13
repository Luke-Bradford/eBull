/**
 * Validates that a URL string uses a safe scheme (http or https).
 *
 * Returns the URL unchanged if valid, or null if the scheme is
 * dangerous (e.g. javascript:, data:, vbscript:).  Used to sanitise
 * external URLs from the API before rendering them as href attributes.
 */
export function safeExternalUrl(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return url;
    }
    return null;
  } catch {
    // Relative URLs or malformed strings — reject.
    return null;
  }
}
