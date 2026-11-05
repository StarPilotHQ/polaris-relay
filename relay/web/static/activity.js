(function () {
  "use strict";
  let open = [];
  document.addEventListener("htmx:beforeSwap", () => {
    open = [...document.querySelectorAll("details[id][open]")].map((d) => d.id);
  });
  document.addEventListener("htmx:afterSwap", () => {
    for (const id of open) document.getElementById(id)?.setAttribute("open", "");
  });
})();
