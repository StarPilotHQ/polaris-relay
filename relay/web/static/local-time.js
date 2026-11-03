(function () {
  "use strict";
  const styles = {
    date: { dateStyle: "medium" },
    datetime: { dateStyle: "medium", timeStyle: "short" },
    time: { timeStyle: "short" },
  };
  const full = new Intl.DateTimeFormat(undefined, { dateStyle: "full", timeStyle: "long" });

  function localize() {
    const today = new Date().toDateString();
    for (const el of document.querySelectorAll("time[datetime][data-style]")) {
      const at = new Date(el.dateTime);
      if (Number.isNaN(at.getTime())) continue;
      const style = el.dataset.style === "time" && at.toDateString() !== today ? "datetime" : el.dataset.style;
      el.textContent = new Intl.DateTimeFormat(undefined, styles[style] || styles.datetime).format(at);
      el.title = full.format(at);
    }
  }

  localize();
  document.addEventListener("htmx:afterSwap", localize);
})();
