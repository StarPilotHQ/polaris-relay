(function () {
  "use strict";
  const data = document.getElementById("zone-data");
  const zones = JSON.parse(data.dataset.zones || "[]");
  const map = L.map("map", { worldCopyJump: true });
  L.tileLayer(data.dataset.tiles, {
    maxZoom: 19,
    referrerPolicy: "strict-origin-when-cross-origin",
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);

  function textTip(text) {
    const span = document.createElement("span");
    span.textContent = text;
    return span;
  }

  map.setView([39.5, -98.35], 4);
  const style = { color: "#d63a62", weight: 2, fillOpacity: 0.2 };
  const shapes = zones.map((z) => (z.kind === "circle"
    ? L.circle([z.lat, z.lng], { ...style, radius: z.radius_m })
    : L.polygon(z.points, style)).bindTooltip(textTip(z.name || "Privacy zone")).addTo(map));
  if (shapes.length) map.fitBounds(L.featureGroup(shapes).getBounds().pad(0.3));

  const circleForm = document.getElementById("circle-form");
  const polygonForm = document.getElementById("polygon-form");
  const help = document.getElementById("map-help");
  const draft = { color: "#7849e8", weight: 2, dashArray: "6 4", fillOpacity: 0.15 };
  let mode = "circle", circle = null, corners = [], outline = null;

  function setMode(next) {
    mode = next;
    circleForm.hidden = mode !== "circle";
    polygonForm.hidden = mode !== "polygon";
    document.getElementById("mode-circle").classList.toggle("gx-btn--tonal", mode !== "circle");
    document.getElementById("mode-polygon").classList.toggle("gx-btn--tonal", mode !== "polygon");
    help.textContent = mode === "circle"
      ? "Click the map to place a circle, then adjust its radius and save."
      : "Click each corner in order. Save when the shape is closed around the place.";
  }

  function radius() { return Math.max(25, Number(circleForm.radius_m.value) || 300); }

  function placeCircle(latlng) {
    if (circle) circle.remove();
    circle = L.circle(latlng, { ...draft, radius: radius() }).addTo(map);
    circleForm.lat.value = latlng.lat.toFixed(6);
    circleForm.lng.value = latlng.lng.toFixed(6);
    circleForm.querySelector("[data-save]").disabled = false;
  }

  function drawOutline() {
    if (outline) outline.remove();
    outline = corners.length ? L.polygon(corners, draft).addTo(map) : null;
    polygonForm.points.value = JSON.stringify(corners.map((c) => [Number(c.lat.toFixed(6)), Number(c.lng.toFixed(6))]));
    polygonForm.querySelector("[data-save]").disabled = corners.length < 3;
  }

  map.on("click", (e) => {
    if (mode === "circle") placeCircle(e.latlng);
    else { corners.push(e.latlng); drawOutline(); }
  });
  circleForm.radius_m.addEventListener("input", () => { if (circle) circle.setRadius(radius()); });
  document.getElementById("mode-circle").addEventListener("click", () => setMode("circle"));
  document.getElementById("mode-polygon").addEventListener("click", () => setMode("polygon"));
  document.getElementById("polygon-undo").addEventListener("click", () => { corners.pop(); drawOutline(); });

  const proposals = document.getElementById("proposals");
  document.getElementById("propose").addEventListener("click", async () => {
    proposals.textContent = "Reading the start and end points of your recent drives...";
    let body;
    try {
      const res = await fetch("/zones/proposals", { credentials: "same-origin" });
      body = await res.json();
      if (!res.ok) throw new Error(body.error || "failed");
    } catch (err) {
      proposals.textContent = "Couldn't suggest zones: " + err.message;
      return;
    }
    proposals.textContent = "";
    if (!body.proposals.length) {
      proposals.textContent = "No place shows up often enough to suggest. Draw your zones by hand.";
      return;
    }
    const intro = document.createElement("p");
    intro.className = "gx-note";
    intro.textContent = "Places your drives often start or end. Check each one on the map before adding it:";
    proposals.append(intro);
    body.proposals.forEach((p, i) => {
      const shape = L.circle([p.lat, p.lng], { ...draft, radius: p.radius_m }).addTo(map);
      const row = document.createElement("div");
      row.className = "rl-row";
      const label = document.createElement("span");
      label.textContent = `Place ${i + 1}: ${p.visits} visits, ${p.radius_m} m radius`;
      const show = document.createElement("button");
      show.type = "button";
      show.className = "gx-btn gx-btn--small gx-btn--tonal";
      show.textContent = "Show";
      show.addEventListener("click", () => map.fitBounds(shape.getBounds().pad(0.5)));
      const add = document.createElement("button");
      add.type = "button";
      add.className = "gx-btn gx-btn--small";
      add.textContent = "Add as a zone";
      add.addEventListener("click", () => {
        setMode("circle");
        circleForm.radius_m.value = p.radius_m;
        placeCircle(L.latLng(p.lat, p.lng));
        circleForm.name.value = circleForm.name.value || `Place ${i + 1}`;
        circleForm.requestSubmit();
      });
      row.append(label, show, add);
      proposals.append(row);
    });
  });
  setMode("circle");
})();
