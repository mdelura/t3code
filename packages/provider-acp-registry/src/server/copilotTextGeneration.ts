import { TextGenerationError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { AcpError } from "effect-acp/errors";

import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { findSessionConfigOption } from "@t3tools/provider-acp/server/runtimeModel";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const COPILOT_TIMEOUT_MS = 180_000;
const MAX_OUTPUT_CHARS = 128_000;
const isTextGenerationError = Schema.is(TextGenerationError);

// Copilot CLI 1.0.105: `--available-tools=` restricts nothing, but an allowlist naming only an
// unknown tool leaves the model with none.
const HELPER_ARGS = [
  "--available-tools=t3_no_tools",
  "--no-custom-instructions",
  "--no-ask-user",
  "--disable-builtin-mcps",
  "--no-auto-update",
  "--sync-session=off",
];

type CopilotTextRuntime = Pick<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  | "start"
  | "getEvents"
  | "getConfigOptions"
  | "setModel"
  | "setConfigOption"
  | "prompt"
  | "handleSessionUpdate"
  | "handleRequestPermission"
  | "handleElicitation"
>;

export interface CopilotTextGenerationOptions {
  readonly environment: NodeJS.ProcessEnv;
  readonly makeRuntime: (input: {
    readonly cwd: string;
    readonly args: ReadonlyArray<string>;
    readonly environment: NodeJS.ProcessEnv;
  }) => Effect.Effect<CopilotTextRuntime, AcpError, Scope.Scope>;
}

export const makeCopilotTextGeneration = Effect.fn("makeCopilotTextGeneration")(function* (
  options: CopilotTextGenerationOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const homeDirectory = yield* HostProcess.HomeDirectory;
  const userCopilotHome =
    options.environment.COPILOT_HOME?.trim() || path.join(homeDirectory, ".copilot");

  const runCopilotJson: TextGenerationOperations.Runner = Effect.fn(
    "CopilotTextGeneration.runCopilotJson",
  )(
    function* (input) {
      const { operation } = input;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-text-" });
      // A throwaway home keeps helper sessions out of the user's Copilot history, and their MCP
      // servers, hooks and plugins out of the helper. Machines without a credential store keep
      // the sign-in token in config.json; everywhere else the file may be absent.
      const copilotHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-text-home-" });
      yield* fs
        .copyFile(path.join(userCopilotHome, "config.json"), path.join(copilotHome, "config.json"))
        .pipe(Effect.ignore);

      const rawResult = yield* Effect.gen(function* () {
        const runtime = yield* options.makeRuntime({
          cwd,
          args: HELPER_ARGS,
          environment: { COPILOT_HOME: copilotHome },
        });
        yield* runtime.getEvents().pipe(
          Stream.runForEach((event) =>
            event._tag === "EventStreamBarrier"
              ? Deferred.succeed(event.acknowledge, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
          Effect.forkScoped,
        );

        const output = yield* Ref.make("");
        const rejected = yield* Deferred.make<never, TextGenerationError>();
        const reject = (detail: string) =>
          Deferred.fail(rejected, new TextGenerationError({ operation, detail })).pipe(
            Effect.asVoid,
          );

        yield* runtime.handleRequestPermission(() =>
          reject("Copilot text generation requested a tool permission.").pipe(
            Effect.as({ outcome: { outcome: "cancelled" as const } }),
          ),
        );
        yield* runtime.handleElicitation(() =>
          reject("Copilot text generation requested user input.").pipe(
            Effect.as({ action: "decline" as const }),
          ),
        );
        yield* runtime.handleSessionUpdate(({ update }) =>
          Effect.gen(function* () {
            if (
              update.sessionUpdate === "tool_call" ||
              update.sessionUpdate === "tool_call_update"
            ) {
              return yield* reject("Copilot attempted tool work during text generation.");
            }
            if (update.sessionUpdate !== "agent_message_chunk" || update.content.type !== "text") {
              return;
            }
            const text = update.content.text;
            const exceeded = yield* Ref.modify(output, (current) =>
              current.length + text.length > MAX_OUTPUT_CHARS
                ? [true, current]
                : [false, current + text],
            );
            if (exceeded) {
              return yield* reject("Copilot text generation exceeded the output limit.");
            }
          }),
        );

        return yield* Effect.gen(function* () {
          yield* runtime.start();
          const { model, options: modelOptions } = input.modelSelection;
          if (model.length > 0 && model !== "default") {
            yield* runtime.setModel(model);
          }
          const configOptions = yield* runtime.getConfigOptions;
          for (const selection of modelOptions ?? []) {
            const option = findSessionConfigOption(configOptions, selection.id);
            // Saved options can include mode or allow_all, which must not reach a helper.
            if (option?.category === "thought_level") {
              yield* runtime.setConfigOption(option.id, selection.value);
            }
          }
          const result = yield* runtime.prompt({
            prompt: [{ type: "text", text: input.prompt }],
          });
          if (result.stopReason === "cancelled") {
            return yield* new TextGenerationError({
              operation,
              detail: "Copilot text generation was cancelled.",
            });
          }
          if (yield* Deferred.isDone(rejected)) {
            return yield* Deferred.await(rejected);
          }
          return (yield* Ref.get(output)).trim();
        }).pipe(Effect.raceFirst(Deferred.await(rejected)));
      }).pipe(Effect.scoped);

      if (!rawResult) {
        return yield* new TextGenerationError({
          operation,
          detail: "Copilot returned empty text generation output.",
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(input, "Copilot", rawResult);
    },
    (effect, input) =>
      effect.pipe(
        Effect.timeoutOrElse({
          duration: COPILOT_TIMEOUT_MS,
          orElse: () =>
            Effect.fail(
              new TextGenerationError({
                operation: input.operation,
                detail: "Copilot text generation timed out.",
              }),
            ),
        }),
        Effect.scoped,
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation: input.operation,
                detail:
                  "_tag" in cause && cause._tag === "AcpRequestError" && cause.code === -32000
                    ? "Sign in to GitHub Copilot in provider settings."
                    : "Copilot text generation failed.",
                cause,
              }),
        ),
      ),
  );

  return TextGenerationOperations.fromRunner(
    "CopilotTextGeneration",
    TextGenerationOperations.withoutInvestigation("Copilot", runCopilotJson),
  );
});
