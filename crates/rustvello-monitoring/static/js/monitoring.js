(function (global) {
  "use strict";

  document.addEventListener("click", function (event) {
    var target = event.target;
    if (!target || !target.closest) return;
    var header = target.closest(".monitor-filter-header");
    if (
      !header ||
      target.closest(
        "a, button, input, select, textarea, label, .monitor-filter-chip",
      )
    )
      return;
    var toggle = header.querySelector('[data-bs-toggle="collapse"]');
    if (toggle) toggle.click();
  });

  function apply(url, values) {
    Object.keys(values || {}).forEach(function (key) {
      var value = values[key];
      if (value === null || value === undefined || value === "") {
        url.searchParams.delete(key);
      } else {
        url.searchParams.set(key, String(value));
      }
    });
    return url.pathname + (url.search ? url.search : "");
  }

  function timeline(values) {
    return apply(
      new URL("/invocations/timeline", global.location.origin),
      values,
    );
  }

  function timelineFromCurrent(values) {
    var url = new URL(global.location.href);
    url.pathname = "/invocations/timeline";
    url.hash = "";
    return apply(url, values);
  }

  function fitWindow(startMs, endMs, targetFill) {
    var fill = Math.min(1, Math.max(0.1, targetFill || 0.82));
    var actualSpan = Math.max(0, endMs - startMs);
    var selectionSpan = Math.max(actualSpan, 10);
    var center = actualSpan > 0 ? startMs + actualSpan / 2 : startMs;
    var viewportSpan = selectionSpan / fill;
    return {
      start: new Date(center - viewportSpan / 2),
      end: new Date(center + viewportSpan / 2),
    };
  }

  global.RustvelloMonitoringLinks = {
    fitWindow: fitWindow,
    timeline: timeline,
    timelineFromCurrent: timelineFromCurrent,
  };

  var workflowRequest;
  var workflowUrl;
  var workflowColumns = "2";
  var workflowLegends = true;

  function workflowLayout(page) {
    page.querySelector(".workflow-comparison-grid").dataset.columns =
      workflowColumns;
    page.classList.toggle("workflow-hide-legends", !workflowLegends);
    page.querySelector("[data-workflow-legends]").checked = workflowLegends;
    page.querySelectorAll("[data-workflow-columns]").forEach(function (button) {
      var active = button.dataset.workflowColumns === workflowColumns;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
  }

  async function loadWorkflow(url, push) {
    var page = document.querySelector("[data-workflow-comparison]");
    if (!page) return;
    if (workflowRequest) workflowRequest.abort();
    var request = new AbortController();
    var focusedRun = document.activeElement.dataset.workflowId;
    workflowRequest = request;
    workflowUrl = url;
    page.setAttribute("aria-busy", "true");
    try {
      var response = await fetch(url, { signal: request.signal });
      if (!response.ok)
        throw new Error("Unable to load runs (" + response.status + ").");
      var html = new DOMParser().parseFromString(
        await response.text(),
        "text/html",
      );
      if (request.signal.aborted) return;
      var replacement = html.querySelector("[data-workflow-comparison]");
      if (!replacement) throw new Error("Workflow response is incomplete.");
      page.replaceWith(replacement);
      workflowLayout(replacement);
      if (focusedRun) {
        var focusedRow = Array.from(
          replacement.querySelectorAll("[data-workflow-id]"),
        ).find(function (row) {
          return row.dataset.workflowId === focusedRun;
        });
        if (focusedRow) focusedRow.focus({ preventScroll: true });
      }
      if (push !== false) history.pushState(null, "", url);
      workflowUrl = null;
      document.body.dispatchEvent(new Event("htmx:afterSwap"));
    } catch (error) {
      if (error.name !== "AbortError") {
        page.querySelector("[data-workflow-error]").textContent = error.message;
        workflowUrl = null;
      }
    } finally {
      if (workflowRequest === request) page.removeAttribute("aria-busy");
    }
  }

  function workflowSelection(page, id, clear) {
    var url = new URL(workflowUrl || global.location.href);
    var raw = workflowUrl
      ? url.searchParams.get("histogram_workflow")
      : page.querySelector("#workflow-selection-input").value;
    var selected = new Set((raw || "").split(",").filter(Boolean));
    if (clear) selected.clear();
    else if (selected.has(id)) selected.delete(id);
    else if (selected.size < 10) selected.add(id);
    else {
      page.querySelector("[data-workflow-error]").textContent =
        "Select up to 10 runs.";
      return;
    }
    url.searchParams.set("histogram_workflow", Array.from(selected).join(","));
    page.querySelector("#workflow-selection-input").value =
      Array.from(selected).join(",");
    page.querySelectorAll("[data-workflow-id]").forEach(function (row) {
      var active = selected.has(row.dataset.workflowId);
      row.classList.toggle("table-primary", active);
      row.setAttribute("aria-selected", String(active));
    });
    loadWorkflow(url);
  }

  document.addEventListener("click", function (event) {
    var page = event.target.closest("[data-workflow-comparison]");
    if (!page) return;
    var columns = event.target.closest("[data-workflow-columns]");
    if (columns) {
      workflowColumns = columns.dataset.workflowColumns;
      workflowLayout(page);
      return;
    }
    if (event.target.closest("[data-workflow-clear]")) {
      workflowSelection(page, null, true);
      return;
    }
    if (event.target.closest("[data-workflow-refresh]")) {
      loadWorkflow(new URL(global.location.href));
      return;
    }
    var remove = event.target.closest("[data-workflow-remove]");
    if (remove) {
      workflowSelection(page, remove.dataset.workflowRemove);
      return;
    }
    var row = event.target.closest("[data-workflow-id]");
    if (row && !event.target.closest("a,button,input")) {
      workflowSelection(page, row.dataset.workflowId);
      return;
    }
    var link = event.target.closest(".pagination a");
    if (
      link &&
      !event.ctrlKey &&
      !event.metaKey &&
      !event.shiftKey &&
      !event.altKey
    ) {
      event.preventDefault();
      var destination = new URL(link.href);
      destination.searchParams.set(
        "histogram_workflow",
        page.querySelector("#workflow-selection-input").value,
      );
      destination.searchParams.set(
        "histogram_status",
        page.querySelector('input[name="histogram_status"]').value,
      );
      loadWorkflow(destination);
    }
  });
  document.addEventListener("keydown", function (event) {
    if (
      (event.key === "Enter" || event.key === " ") &&
      event.target.matches("[data-workflow-id]")
    ) {
      event.preventDefault();
      workflowSelection(
        event.target.closest("[data-workflow-comparison]"),
        event.target.dataset.workflowId,
      );
    }
  });
  document.addEventListener("change", function (event) {
    var page = event.target.closest("[data-workflow-comparison]");
    if (!page) return;
    if (event.target.matches("[data-workflow-legends]")) {
      workflowLegends = event.target.checked;
      workflowLayout(page);
    }
    if (event.target.matches("[data-workflow-page-size]")) {
      var url = new URL(global.location.href);
      url.searchParams.set("limit", event.target.value);
      url.searchParams.set("page", "1");
      url.searchParams.set(
        "histogram_workflow",
        page.querySelector("#workflow-selection-input").value,
      );
      loadWorkflow(url);
    }
  });
  document.addEventListener("submit", function (event) {
    if (event.target.id !== "workflow-selection-form") return;
    event.preventDefault();
    var url = new URL(workflowUrl || global.location.href);
    new FormData(event.target).forEach(function (value, key) {
      url.searchParams.set(key, value);
    });
    loadWorkflow(url);
  });
  global.addEventListener("popstate", function () {
    if (document.querySelector("[data-workflow-comparison]"))
      loadWorkflow(new URL(global.location.href), false);
  });
})(window);
