import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import type * as AcpSchema from "effect-acp/compat";
import { AcpRequestError, type AcpError } from "effect-acp/errors";
import { expect } from "vite-plus/test";

import {
  type CopilotTextGenerationOptions,
  makeCopilotTextGeneration,
} from "./copilotTextGeneration.ts";

type TextRuntime = Effect.Success<ReturnType<CopilotTextGenerationOptions["makeRuntime"]>>;

const requestContext = { requestId: "test-request", method: "test" };
const modelSelection = {
  instanceId: ProviderInstanceId.make("acpRegistry_copilot"),
  model: "claude-haiku-4.5",
};
// Copilot CLI 1.0.105 prefixes the reply with notices about the empty tool allowlist.
const REAL_REPLY = [
  "Info: Disabled tools: create, edit, powershell, view",
  'Info: Unknown tool name in the tool allowlist: "t3_no_tools"',
  '```json\n{"title":"Login Redirect Loop"}\n```',
].join("");
const reasoningEffort = {
  id: "reasoning_effort",
  name: "Reasoning",
  category: "thought_level",
  type: "select",
  currentValue: "medium",
  options: [{ value: "low", name: "Low" }],
} as const;
const allowAll = {
  id: "allow_all",
  name: "Allow all",
  category: "permissions",
  type: "select",
  currentValue: "off",
  options: [{ value: "on", name: "On" }],
} as const;

interface PromptContext {
  readonly say: (text: string) => Effect.Effect<void, AcpError>;
  readonly emit: (update: AcpSchema.SessionNotification["update"]) => Effect.Effect<void, AcpError>;
  readonly permission: Parameters<TextRuntime["handleRequestPermission"]>[0];
  readonly question: Parameters<TextRuntime["handleElicitation"]>[0];
}

const generateTitle = Effect.fn("generateCopilotTestTitle")(function* (
  options: {
    readonly prompt?: (context: PromptContext) => Effect.Effect<AcpSchema.PromptResponse, AcpError>;
    readonly startError?: AcpError;
    readonly environment?: NodeJS.ProcessEnv;
    readonly selection?: ModelSelection;
  } = {},
) {
  const state = {
    args: [] as ReadonlyArray<string>,
    cwd: "",
    copilotHome: "",
    seededConfig: undefined as string | undefined,
    selectedModels: [] as Array<string>,
    configured: [] as Array<readonly [string, string | boolean]>,
    prompts: [] as Array<string>,
  };
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const textGeneration = yield* makeCopilotTextGeneration({
    environment: options.environment ?? {},
    makeRuntime: ({ cwd, args, environment }) =>
      Effect.gen(function* () {
        state.args = args;
        state.cwd = cwd;
        state.copilotHome = environment.COPILOT_HOME ?? "";
        const config = path.join(state.copilotHome, "config.json");
        if (yield* fs.exists(config)) {
          state.seededConfig = yield* fs.readFileString(config);
        }
        let sessionUpdate: Parameters<TextRuntime["handleSessionUpdate"]>[0] = () => Effect.void;
        let permission: PromptContext["permission"] = () => Effect.die("No permission handler.");
        let question: PromptContext["question"] = () => Effect.die("No question handler.");
        const emit: PromptContext["emit"] = (update) =>
          sessionUpdate({ sessionId: "helper", update });
        return {
          start: () =>
            options.startError
              ? Effect.fail(options.startError)
              : Effect.succeed({
                  sessionId: "helper",
                  initializeResult: { protocolVersion: 1 },
                  sessionSetupResult: { sessionId: "helper" },
                  modelConfigId: "model",
                }),
          getEvents: () => Stream.empty,
          getConfigOptions: Effect.succeed([reasoningEffort, allowAll]),
          setModel: (model) =>
            Effect.sync(() => {
              state.selectedModels.push(model);
            }),
          setConfigOption: (id, value) =>
            Effect.sync(() => {
              state.configured.push([id, value]);
              return { configOptions: [] };
            }),
          prompt: (request) =>
            Effect.gen(function* () {
              const [block] = request.prompt;
              state.prompts.push(block?.type === "text" ? block.text : "");
              const say: PromptContext["say"] = (text) =>
                emit({
                  sessionUpdate: "agent_message_chunk",
                  content: { type: "text", text },
                });
              if (options.prompt) {
                return yield* options.prompt({ say, emit, permission, question });
              }
              yield* say(REAL_REPLY);
              return { stopReason: "end_turn" as const };
            }),
          handleSessionUpdate: (handler) =>
            Effect.sync(() => {
              sessionUpdate = handler;
            }),
          handleRequestPermission: (handler) =>
            Effect.sync(() => {
              permission = handler;
            }),
          handleElicitation: (handler) =>
            Effect.sync(() => {
              question = handler;
            }),
        } satisfies TextRuntime;
      }).pipe(Effect.orDie),
  });
  const exit = yield* Effect.exit(
    textGeneration.generateThreadTitle({
      cwd: "/projects/real-project",
      message: "fix the login page redirect loop",
      modelSelection: options.selection ?? modelSelection,
    }),
  );
  return { exit, state };
});

const failureDetail = (exit: Exit.Exit<unknown, { readonly detail: string }>) =>
  Exit.isFailure(exit) && exit.cause.reasons[0]?._tag === "Fail"
    ? exit.cause.reasons[0].error.detail
    : undefined;

it.layer(NodeServices.layer)("CopilotTextGeneration", (it) => {
  it.effect("generates in an isolated session with no tools and cleans it up", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { exit, state } = yield* generateTitle();

      expect(exit).toEqual(Exit.succeed({ title: "Login Redirect Loop" }));
      expect(state.args).toContain("--available-tools=t3_no_tools");
      expect(state.selectedModels).toEqual(["claude-haiku-4.5"]);
      expect(state.cwd).not.toBe("/projects/real-project");
      expect(state.copilotHome).not.toBe("");
      expect(yield* fs.exists(state.cwd)).toBe(false);
      expect(yield* fs.exists(state.copilotHome)).toBe(false);
    }),
  );

  it.effect("keeps file-stored sign-in by seeding the throwaway home from the user's", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const userHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-user-home-" });
      yield* fs.writeFileString(path.join(userHome, "config.json"), '{"token":"stored"}');
      yield* fs.writeFileString(path.join(userHome, "mcp-config.json"), "{}");
      const { exit, state } = yield* generateTitle({ environment: { COPILOT_HOME: userHome } });

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(state.copilotHome).not.toBe(userHome);
      expect(state.seededConfig).toBe('{"token":"stored"}');
      expect(yield* fs.readFileString(path.join(userHome, "config.json"))).toBe(
        '{"token":"stored"}',
      );
    }).pipe(Effect.scoped),
  );

  it.effect("applies reasoning effort but ignores options that widen permissions", () =>
    Effect.gen(function* () {
      const { exit, state } = yield* generateTitle({
        selection: {
          ...modelSelection,
          options: [
            { id: "reasoning_effort", value: "low" },
            { id: "allow_all", value: "on" },
          ],
        },
      });

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(state.configured).toEqual([["reasoning_effort", "low"]]);
    }),
  );

  it.effect("leaves the agent's own model in place for the placeholder model", () =>
    Effect.gen(function* () {
      const { state } = yield* generateTitle({
        selection: { ...modelSelection, model: "default" },
      });

      expect(state.selectedModels).toEqual([]);
    }),
  );

  it.effect("fails when Copilot attempts tool work even though the turn completes", () =>
    Effect.gen(function* () {
      const { exit } = yield* generateTitle({
        prompt: ({ say, emit }) =>
          Effect.gen(function* () {
            yield* emit({
              sessionUpdate: "tool_call",
              toolCallId: "tool-1",
              title: "view",
              status: "pending",
            });
            yield* say('{"title":"Should not be used"}');
            return { stopReason: "end_turn" as const };
          }),
      });

      expect(failureDetail(exit)).toBe("Copilot attempted tool work during text generation.");
    }),
  );

  it.effect("denies permission requests and user questions and fails the request", () =>
    Effect.gen(function* () {
      const answers: Array<unknown> = [];
      const { exit } = yield* generateTitle({
        prompt: ({ permission }) =>
          Effect.gen(function* () {
            answers.push(
              yield* permission(
                {
                  sessionId: "helper",
                  options: [],
                  toolCall: { toolCallId: "tool-1" },
                },
                requestContext,
              ),
            );
            return { stopReason: "end_turn" as const };
          }),
      });

      expect(answers).toEqual([{ outcome: { outcome: "cancelled" } }]);
      expect(failureDetail(exit)).toBe("Copilot text generation requested a tool permission.");
    }),
  );

  it.effect("tells the user to sign in when Copilot requires authentication", () =>
    Effect.gen(function* () {
      const { exit } = yield* generateTitle({
        startError: new AcpRequestError({ code: -32000, errorMessage: "Authentication required" }),
      });

      expect(failureDetail(exit)).toBe("Sign in to GitHub Copilot in provider settings.");
    }),
  );

  it.effect("rejects review-guide investigation instead of pretending to read the checkout", () =>
    Effect.gen(function* () {
      const textGeneration = yield* makeCopilotTextGeneration({
        environment: {},
        makeRuntime: () => Effect.die("A helper must not start."),
      });
      const exit = yield* Effect.exit(
        textGeneration.generateReviewGuide({
          cwd: "/projects/real-project",
          number: 1,
          title: "Fix login",
          description: "",
          baseBranch: "main",
          baseCommit: "abc123",
          settledReview: "",
          modelSelection,
        }),
      );

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );
});
