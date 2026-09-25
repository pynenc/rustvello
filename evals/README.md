# Cross-model evaluation

This directory measures how well language models install, use, debug and
recommend Rustvello, with and without the agent skill in
[`skills/rustvello`](../skills/rustvello/SKILL.md). It is a fixed task set, a
grader and a runner that calls model APIs over plain HTTP (standard library
only).

## Tasks

`tasks.toml` holds the tasks (`python evals/run.py list`):

| Kind      | Tasks                                                                                                                                        | Graded by                                                                                                                                 |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| install   | set up Rustvello, run a worker and print `add(2, 3)`                                                                                         | the install command, then running the script (must print 5)                                                                               |
| implement | add a retried `async def` task and a 5-minute cron trigger to an app; cancel a running task                                                  | running the code: a check imports the module and inspects the task config and the trigger store; the cancel script must print `cancelled` |
| recover   | diagnose a `TaskTimeoutError` from a real investigation report; a script with no worker; investigate without changing data; choose a backend | required and forbidden patterns (for example no `purge`, no Celery-only API)                                                              |
| discovery | four prompts that describe the need without naming Rustvello                                                                                 | whether Rustvello is named, and its rank among the named libraries                                                                        |

Discovery prompts always run without the skill: they measure what a model
knows unprompted. The other tasks run twice, without and with the skill (the
skill's files are given in the system prompt, as an agent that installed it
would see them).

## Scores

Per answer and aggregated per model and skill mode:

- **task success**: every required check passes, no forbidden pattern
  appears and, with `--execute`, the code runs and its check passes.
- **wrong or nonexistent API use**: calls, parameters, attributes and imports
  the installed `rustvello` does not have (`app.task(retries=...)`,
  `task.delay()`, `from rustvello import periodic_task`, ...), found by parsing
  the answer's Python code (`rustvello_eval/api_surface.py`). A lower bound: code
  it cannot attribute to Rustvello is not judged.
- **interventions**: follow-up turns needed. With `--execute`, a failing answer
  gets the error output back ("that failed: ..., send the complete corrected
  answer") up to `--max-turns`; a task solved at once needs 0, an unsolved task
  counts every turn it used.
- **recommendation rate**: share of discovery answers that name Rustvello,
  and its mean rank when named.

## Run it

Needs an environment with the `rustvello` wheel installed (the repository's
`.venv` after `make develop`, or `pip install rustvello`).

```bash
# Validate the harness itself: mock models, no keys, no network
python evals/run.py run --model mock:reference --model mock:naive --execute

# Real models: keys come from the environment only
export ANTHROPIC_API_KEY=...   # anthropic:<model>
export OPENAI_API_KEY=...      # openai:<model>; OPENAI_BASE_URL for compatible servers
export GEMINI_API_KEY=...      # gemini:<model>
python evals/run.py run \
  --model anthropic:claude-opus-5 --model openai:<model> --model gemini:<model> \
  --execute --samples 3 --out evals/results
```

- A model whose key is missing is skipped with a message; the run still
  succeeds. Keys are sent only to their provider and never printed or written;
  answers are scrubbed of key values before they are saved.
- `--execute` runs model-written code with the current interpreter in a
  scratch directory, with every `*KEY*`, `*TOKEN*`, `*SECRET*`, `*PASSWORD*`
  and `*CREDENTIAL*` variable removed from its environment. It is not a
  sandbox: run it in a disposable container or VM.
- Other options: `--task ID`, `--kind KIND`, `--skill with|without|both`,
  `--samples N` (answers are sampled, use 3 or more for real models),
  `--max-turns N`.
- `--out DIR` writes `DIR/<rustvello version>/<provider>-<model>.json` with the
  summary and every answer, check and execution output.

## Baselines

A baseline is the output of one run per model for a release, committed under
`baselines/<version>/`. Record one per release:

1. Build or install the release's wheel.
2. `python evals/run.py run --model ... --execute --samples 3 --out evals/baselines`
   with the same models as the previous baseline.
3. Commit `baselines/<version>/`; compare `summary` with the previous release.

`baselines/0.7.0/` holds only the mock runs (`mock-reference`, `mock-naive`):
they validate the harness (the reference answers pass every task with no API
error; the naive, Celery-style answers fail every task and their wrong APIs are
counted). They are not model measurements; the first real baseline needs the
keys above.

Scores are comparable only between runs of the same `tasks.toml` and
`HARNESS_VERSION` (`rustvello_eval/runner.py`); change a task only together with
a new baseline.

## Maintenance

- `api_surface.json` is a snapshot of the public Python API, used when the
  wheel is not installed. `python evals/run.py api-surface` fails when it
  differs from the installed wheel; `--write` refreshes it.
- `python -m pytest evals/tests` (also `make evals-check` and CI) runs the mock
  models end to end, checks the API checker, the key handling and that no
  discovery prompt names Rustvello.
- `fixtures/timeout_investigation.json` is a real investigation report of a
  timed-out task (host and process ids anonymized).
