export function globalize(variables: Record<string, unknown>): void {
  for (const key in variables) {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    window[key] = variables[key];
  }
}
