# Workflow comparison

Open **Workflows**, then a workflow task. Click a run row, or focus it and press
Enter or Space, to toggle comparison. Selection persists across pages and is
encoded in the URL. Up to ten runs can be selected. Clear selection also
persists after a reload. The initial selection shows up to three long-running
roots from the current page.

Use the full run ID to open its root invocation, **Invocations** for members,
or the timeline icon for the entire run. A direct workflow-run link opens the
page containing that run. The run list reports root duration; comparison panels
use the loaded member histories to determine their elapsed window.

Charts align at elapsed zero and share duration, bucket resolution, task count,
and worker count scales. Task colors remain stable between runs. Hovering a
bucket updates the legends in all selected panels at the same elapsed position.
Choose one or two charts per row and optionally hide legends. Narrow screens
use one column and independently scroll wide tables/charts.

## Cost and freshness

- Unselected runs load root metadata/history and member counts, not every member's history.
- Selected runs load at most 2,000 member IDs plus a missing root, and retain at
  most 20,000 history entries per run. A visible notice marks incomplete data;
  partial charts must not be interpreted as complete workflow totals.
- Raw selected histories are cached per application/run for two seconds, with
  at most ten cached snapshots. Changing status filters reuses those histories.
  Live changes may take two seconds to appear. Counts and root metadata are not
  cached. A single backend history read can still exceed the retained-entry limit.
- Plain workflow-member lists page in storage. Adding other filters can use the
  older invocation filtering path; this is not a claim of fully indexed filtering
  for every query combination.
- SQLite, PostgreSQL, and MongoDB use indexed membership pages. Redis member
  pages transfer only the requested IDs, but SORT still processes the set on the
  Redis server. Redis workflow-run paging retains its compatibility implementation.
- Offset pagination has a stable ID order. Concurrent inserts can shift page
  boundaries; it is not a snapshot or cursor-pagination contract.

See [monitoring fixtures](monitoring-test-fixtures.md) for the reproducible
comparison fixture and browser checks.
