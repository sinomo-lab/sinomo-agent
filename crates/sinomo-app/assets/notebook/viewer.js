// This is the only executable asset in the notebook preview. All notebook data,
// including HTML results, comes from the inert JSON blocks in the document.
(() => {
  "use strict";
  const nb = JSON.parse(document.getElementById("notebook").textContent);
  const markdown = JSON.parse(document.getElementById("markdown").textContent);
  const root = document.getElementById("cells");
  const summary = document.getElementById("summary");
  const next = document.getElementById("next");
  const errors = [];
  let errorIndex = -1;

  function lines(value) {
    if (typeof value === "string") return value;
    return Array.isArray(value) && value.every(s => typeof s === "string") ? value.join("") : "";
  }
  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function boundedText(text) {
    const limit = 131072;
    return text.length <= limit ? text : text.slice(0, limit) + "\n… Preview shortened. See Source for the complete text.";
  }
  function plain(text) {
    // Jupyter tracebacks and captured terminal output commonly contain ANSI colors.
    return element("pre", "", boundedText(text).replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, ""));
  }
  function image(bundle, alt) {
    for (const mime of ["image/png", "image/jpeg", "image/svg+xml", "image/gif", "image/webp"]) {
      const value = lines(bundle?.[mime]);
      if (!value) continue;
      const img = element("img");
      img.alt = alt;
      img.loading = "lazy";
      img.src = mime === "image/svg+xml"
        ? "data:image/svg+xml;charset=utf-8," + encodeURIComponent(value)
        : "data:" + mime + ";base64," + value.replace(/\s/g, "");
      return img;
    }
    return null;
  }

  // Copy only passive markup into the viewer document. Never move an input
  // node, or copy its styles, handlers, IDs, URLs or arbitrary attributes.
  const tags = new Set("p div span strong b em i s del u small sub sup code pre kbd blockquote ul ol li h1 h2 h3 h4 h5 h6 hr br table caption thead tbody tfoot tr td th figure figcaption dl dt dd".split(" "));
  const skip = new Set("script style iframe frame object embed link meta base template noscript audio video form".split(" "));
  function rich(html, attachments) {
    if (html.length > 1048576) return element("p", "label", "Rich output exceeds the preview limit. See Source for the complete content.");
    const template = document.createElement("template");
    template.innerHTML = html;
    const result = element("div", "markdown");
    let budget = 20000;
    let limited = false;
    function copy(parent, input, depth) {
      if (--budget < 0 || depth > 64) { limited = true; return; }
      if (input.nodeType === Node.TEXT_NODE) {
        parent.append(document.createTextNode(input.data));
        return;
      }
      if (input.nodeType !== Node.ELEMENT_NODE || input.namespaceURI !== "http://www.w3.org/1999/xhtml") return;
      const tag = input.localName;
      if (skip.has(tag)) return;
      if (tag === "img") {
        const src = input.getAttribute("src") || "";
        const alt = input.getAttribute("alt") || "Notebook image";
        let img;
        if (src.startsWith("attachment:")) {
          let key = src.slice(11);
          try { key = decodeURIComponent(key); } catch {}
          img = image(attachments?.[key], alt);
        } else {
          try {
            const url = new URL(src, location.href);
            const directory = location.pathname.slice(0, location.pathname.lastIndexOf("/") + 1);
            if (src && ((url.origin === location.origin && url.pathname.startsWith(directory)) ||
                /^data:image\/(png|jpeg|gif|webp);base64,/i.test(src))) {
              img = element("img"); img.alt = alt; img.loading = "lazy"; img.src = url.href;
            }
          } catch {}
        }
        if (img) parent.append(img);
        else if (input.getAttribute("alt")) parent.append(document.createTextNode(alt));
        return;
      }
      const child = tags.has(tag) ? element(tag) : parent;
      if (child !== parent) {
        if (tag === "th" || tag === "td") {
          for (const attr of ["rowspan", "colspan"]) {
            const n = Number(input.getAttribute(attr));
            if (Number.isInteger(n) && n > 1 && n <= 100) child.setAttribute(attr, n);
          }
        }
        parent.append(child);
      }
      for (const descendant of input.childNodes) {
        if (budget <= 0) { limited = true; break; }
        copy(child, descendant, depth + 1);
      }
    }
    for (const node of template.content.childNodes) {
      if (budget <= 0) { limited = true; break; }
      copy(result, node, 0);
    }
    if (limited) result.append(element("p", "label", "Rich output shortened. See Source for the complete result."));
    return result;
  }
  function representation(bundle) {
    const html = lines(bundle?.["text/html"]);
    if (html) {
      const rendered = rich(html);
      if (rendered.textContent.trim() || rendered.querySelector("table,img")) return rendered;
    }
    const plot = image(bundle, "Saved plot");
    if (plot) return plot;
    if (bundle && "text/plain" in bundle) return plain(lines(bundle["text/plain"]));
    if (bundle && "application/json" in bundle) return plain(JSON.stringify(bundle["application/json"], null, 2));
    return element("p", "label", "Unsupported saved output: " + (Object.keys(bundle || {}).join(", ") || "empty MIME bundle"));
  }

  function output(item) {
    const box = element("div", "output");
    if (item?.output_type === "error") {
      box.classList.add("error");
      box.tabIndex = -1;
      box.append(element("div", "label", [item.ename, item.evalue].filter(Boolean).join(": ") || "Error"));
      const trace = Array.isArray(item.traceback) ? item.traceback.join("\n") : lines(item.traceback);
      if (trace) box.append(plain(trace));
      errors.push(box);
    } else if (item?.output_type === "stream") {
      const stderr = item.name === "stderr";
      if (stderr) box.classList.add("stderr");
      box.append(element("div", "label", stderr ? "stderr" : "stdout"), plain(lines(item.text)));
    } else if (item?.output_type === "display_data" || item?.output_type === "execute_result") {
      const data = item.data && typeof item.data === "object" ? item.data : {};
      box.append(representation(data));
    } else {
      box.append(element("p", "label", "Unsupported output type: " + (item?.output_type || "unknown")));
    }
    return box;
  }

  function cellView(cell, i) {
    const section = element("section");
    section.id = "cell-" + (i + 1);
    const source = lines(cell.source);
    const kind = cell.cell_type;
    const prompt = kind === "code" ? "In [" + (cell.execution_count ?? " ") + "]" : kind;
    section.append(element("div", "label", "Cell " + (i + 1) + " · " + prompt));
    if (kind === "markdown") section.append(rich(markdown[i], cell.attachments));
    else section.append(element("pre", kind === "code" ? "code" : "raw", boundedText(source)));
    if (kind === "code" && Array.isArray(cell.outputs)) {
      for (const item of cell.outputs.slice(0, 1000)) {
        try { section.append(output(item)); }
        catch { section.append(element("p", "label", "Malformed saved output. See Source for its contents.")); }
      }
      if (cell.outputs.length > 1000) section.append(element("p", "label", "Showing the first 1,000 saved outputs for this cell."));
    }
    return section;
  }

  function describe() {
    const name = nb.metadata?.kernelspec?.display_name;
    summary.textContent = [typeof name === "string" ? name : "", nb.cells.length + (nb.cells.length === 1 ? " cell" : " cells"),
      errors.length ? errors.length + (errors.length === 1 ? " saved error" : " saved errors") : ""].filter(Boolean).join(" · ");
    next.hidden = !errors.length;
  }
  next.addEventListener("click", () => {
    errorIndex = (errorIndex + 1) % errors.length;
    errors[errorIndex].focus({preventScroll:true});
    errors[errorIndex].scrollIntoView({block:"start"});
  });
  let index = 0;
  function renderBatch() {
    const until = performance.now() + 12;
    const batch = document.createDocumentFragment();
    while (index < nb.cells.length) {
      batch.append(cellView(nb.cells[index], index));
      index++;
      if (performance.now() > until) break;
    }
    root.append(batch);
    describe();
    if (index < nb.cells.length) requestAnimationFrame(renderBatch);
  }
  if (!nb.cells.length) root.append(element("p", "", "This notebook has no cells."));
  renderBatch();
})();
