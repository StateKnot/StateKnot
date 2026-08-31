// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

const commandDialog = document.querySelector<HTMLDialogElement>(
  "[data-command-dialog]",
);
const commandOpen = document.querySelector<HTMLButtonElement>(
  "[data-command-open]",
);
const commandClose = document.querySelector<HTMLButtonElement>(
  "[data-command-close]",
);
const commandInput = document.querySelector<HTMLInputElement>(
  "[data-command-input]",
);
const commandCount = document.querySelector<HTMLElement>(
  "[data-command-count]",
);
const commandResults = Array.from(
  document.querySelectorAll<HTMLAnchorElement>("[data-command-result]"),
);

let visibleResults = [...commandResults];
let activeResult = 0;
const isChinese = document.documentElement.lang
  .toLocaleLowerCase()
  .startsWith("zh");

const setActiveResult = (index: number): void => {
  if (!commandInput || visibleResults.length === 0) {
    commandInput?.removeAttribute("aria-activedescendant");
    return;
  }

  activeResult = (index + visibleResults.length) % visibleResults.length;

  for (const [resultIndex, result] of visibleResults.entries()) {
    const isActive = resultIndex === activeResult;
    result.classList.toggle("is-active", isActive);
    result.setAttribute("aria-selected", String(isActive));
  }

  const selected = visibleResults[activeResult];
  if (selected) {
    commandInput.setAttribute("aria-activedescendant", selected.id);
    selected.scrollIntoView({ block: "nearest" });
  }
};

const filterCommands = (): void => {
  if (!commandInput || !commandCount) return;

  const query = commandInput.value.trim().toLocaleLowerCase();
  visibleResults = commandResults.filter((result) => {
    const haystack = result.dataset.commandSearch?.toLocaleLowerCase() ?? "";
    const matches = haystack.includes(query);
    result.hidden = !matches;
    result.classList.remove("is-active");
    result.setAttribute("aria-selected", "false");
    return matches;
  });

  commandCount.textContent = isChinese
    ? `${visibleResults.length} 项结果`
    : `${visibleResults.length} ${
        visibleResults.length === 1 ? "result" : "results"
      }`;
  activeResult = 0;
  setActiveResult(0);
};

const openCommands = (): void => {
  if (!commandDialog || !commandInput || commandDialog.open) return;

  commandDialog.showModal();
  commandInput.value = "";
  filterCommands();
  commandInput.focus({ preventScroll: true });
};

const closeCommands = (): void => {
  if (commandDialog?.open) commandDialog.close();
};

commandOpen?.addEventListener("click", openCommands);
commandClose?.addEventListener("click", closeCommands);
commandInput?.addEventListener("input", filterCommands);

commandInput?.addEventListener("keydown", (event) => {
  if (event.key === "ArrowDown") {
    event.preventDefault();
    setActiveResult(activeResult + 1);
  }

  if (event.key === "ArrowUp") {
    event.preventDefault();
    setActiveResult(activeResult - 1);
  }

  if (event.key === "Enter") {
    const selected = visibleResults[activeResult];
    if (selected) {
      event.preventDefault();
      selected.click();
    }
  }
});

commandDialog?.addEventListener("click", (event) => {
  if (event.target === commandDialog) closeCommands();
});

commandDialog?.addEventListener("close", () => {
  commandOpen?.focus({ preventScroll: true });
});

for (const result of commandResults) {
  result.addEventListener("click", closeCommands);
}

document.addEventListener("keydown", (event) => {
  if (
    (event.metaKey || event.ctrlKey) &&
    event.key.toLocaleLowerCase() === "k"
  ) {
    event.preventDefault();
    commandDialog?.open ? closeCommands() : openCommands();
  }
});

const copyButtons = Array.from(
  document.querySelectorAll<HTMLButtonElement>("[data-copy-button]"),
);

const copyText = async (text: string): Promise<void> => {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const temporary = document.createElement("textarea");
  temporary.value = text;
  temporary.setAttribute("readonly", "");
  temporary.style.position = "fixed";
  temporary.style.opacity = "0";
  document.body.append(temporary);
  temporary.select();
  const legacyCopy = Reflect.get(document, "execCommand");
  const copied =
    typeof legacyCopy === "function" &&
    Boolean(Reflect.apply(legacyCopy, document, ["copy"]));
  temporary.remove();

  if (!copied) throw new Error("Clipboard copy was rejected");
};

for (const copyButton of copyButtons) {
  copyButton.addEventListener("click", async () => {
    const label = copyButton.querySelector<HTMLElement>("[data-copy-label]");
    const targetId = copyButton.dataset.copyTarget;
    const target = targetId ? document.getElementById(targetId) : null;
    if (!label || !target) return;

    const idleLabel =
      copyButton.dataset.copyIdle ?? (isChinese ? "复制" : "Copy");
    const loadingLabel =
      copyButton.dataset.copyLoading ?? (isChinese ? "复制中" : "Copying");
    const successLabel =
      copyButton.dataset.copySuccess ?? (isChinese ? "已复制" : "Copied");
    const errorLabel =
      copyButton.dataset.copyError ?? (isChinese ? "复制失败" : "Copy failed");

    copyButton.disabled = true;
    copyButton.dataset.state = "loading";
    copyButton.setAttribute("aria-busy", "true");
    label.textContent = loadingLabel;

    try {
      await copyText(target.textContent ?? "");
      copyButton.dataset.state = "success";
      label.textContent = successLabel;
    } catch {
      copyButton.dataset.state = "error";
      label.textContent = errorLabel;
    } finally {
      copyButton.disabled = false;
      copyButton.removeAttribute("aria-busy");
      window.setTimeout(() => {
        delete copyButton.dataset.state;
        label.textContent = idleLabel;
      }, 2_500);
    }
  });
}

const typeLine = document.querySelector<HTMLElement>("[data-type-line]");
const reduceMotion = window.matchMedia(
  "(prefers-reduced-motion: reduce)",
).matches;

if (typeLine && !reduceMotion) {
  const text = typeLine.dataset.text ?? typeLine.textContent ?? "";
  const duration = 550;
  const start = performance.now();

  const typeFrame = (now: number): void => {
    const progress = Math.min((now - start) / duration, 1);
    const length = Math.max(1, Math.floor(text.length * progress));
    typeLine.textContent = text.slice(0, length);

    if (progress < 1) requestAnimationFrame(typeFrame);
  };

  typeLine.textContent = text.slice(0, 1);
  requestAnimationFrame(typeFrame);
}
