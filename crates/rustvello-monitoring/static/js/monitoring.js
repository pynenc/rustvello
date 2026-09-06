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
})(window);
