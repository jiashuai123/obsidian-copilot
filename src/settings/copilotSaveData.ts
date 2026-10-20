import type { CopilotSettings } from "@/settings/model";
import type { App } from "obsidian";

export function getCopilotSaveData(app: App): (data: CopilotSettings) => Promise<void> {
  return async (data: CopilotSettings) => {
    const { plugins } = app as unknown as {
      plugins: {
        getPlugin: (id: string) => { saveData: (data: CopilotSettings) => Promise<void> } | null;
      };
    };
    const virtual = (
      app as unknown as {
        __ignis?: { plugins?: Record<string, { instance?: unknown }> };
      }
    )?.__ignis?.plugins?.["copilot-ignis"]?.instance as
      | { saveData: (data: CopilotSettings) => Promise<void> }
      | undefined;
    const copilotPlugin =
      virtual ?? plugins.getPlugin("copilot-ignis") ?? plugins.getPlugin("copilot");
    if (!copilotPlugin) throw new Error("Copilot plugin not found");
    await copilotPlugin.saveData(data);
  };
}
