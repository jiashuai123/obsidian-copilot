export function isIgnisRuntime(): boolean {
  return typeof window !== "undefined" && !!(window as unknown as { __ignis?: unknown }).__ignis;
}
