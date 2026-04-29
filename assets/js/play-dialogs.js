(function() {
    if (window.playAlert && window.playConfirm) return;

    const STYLE_ID = "play-dialogs-style";
    const ROOT_ID = "play-dialogs-root";

    function ensureStyles() {
        if (document.getElementById(STYLE_ID)) return;

        const style = document.createElement("style");
        style.id = STYLE_ID;
        style.textContent = `
            .play-dialogs-overlay {
                position: fixed;
                inset: 0;
                background: rgba(0, 0, 0, 0.72);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 9000;
                padding: 20px;
                box-sizing: border-box;
            }
            .play-dialogs-box {
                width: 100%;
                max-width: 360px;
                background: linear-gradient(180deg, #173d8f 0%, #0f2a66 100%);
                border: 1px solid rgba(255, 255, 255, 0.2);
                border-radius: 16px;
                box-shadow: 0 18px 40px rgba(0, 0, 0, 0.38);
                overflow: hidden;
                color: white;
                font-family: Arial, sans-serif;
            }
            .play-dialogs-title {
                padding: 16px 18px 10px;
                font-size: 18px;
                font-weight: bold;
                color: #ffeb3b;
                text-align: center;
            }
            .play-dialogs-message {
                padding: 0 18px 18px;
                font-size: 15px;
                line-height: 1.45;
                text-align: center;
                white-space: pre-line;
            }
            .play-dialogs-actions {
                display: flex;
                gap: 10px;
                padding: 0 18px 18px;
            }
            .play-dialogs-btn {
                flex: 1;
                border: none;
                border-radius: 12px;
                font-weight: bold;
                font-size: 15px;
                padding: 13px 14px;
                cursor: pointer;
            }
            .play-dialogs-btn:active {
                transform: scale(0.98);
            }
            .play-dialogs-btn-primary {
                background: #ffeb3b;
                color: #0f2a66;
            }
            .play-dialogs-btn-secondary {
                background: rgba(255, 255, 255, 0.12);
                color: white;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }
        `;
        document.head.appendChild(style);
    }

    function ensureRoot() {
        let root = document.getElementById(ROOT_ID);
        if (root) return root;

        root = document.createElement("div");
        root.id = ROOT_ID;
        root.className = "play-dialogs-overlay";
        root.innerHTML = `
            <div class="play-dialogs-box" tabindex="-1" role="alertdialog" aria-modal="true" aria-labelledby="playDialogsTitle" aria-describedby="playDialogsMessage">
                <div id="playDialogsTitle" class="play-dialogs-title">A Play informa,</div>
                <div id="playDialogsMessage" class="play-dialogs-message"></div>
                <div id="playDialogsActions" class="play-dialogs-actions"></div>
            </div>
        `;
        document.body.appendChild(root);
        return root;
    }

    function getDialogParts() {
        ensureStyles();
        const root = ensureRoot();
        return {
            root,
            box: root.querySelector(".play-dialogs-box"),
            message: document.getElementById("playDialogsMessage"),
            actions: document.getElementById("playDialogsActions")
        };
    }

    function createButton(label, className, onClick) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `play-dialogs-btn ${className}`;
        button.textContent = label;
        button.addEventListener("click", onClick);
        return button;
    }

    function isTextEntryElement(element) {
        if (!(element instanceof HTMLElement)) return false;
        if (element.tagName === "TEXTAREA") return true;
        if (element.tagName !== "INPUT") return !!element.isContentEditable;

        const input = element;
        const type = String(input.type || "text").toLowerCase();
        return !["button", "submit", "reset", "checkbox", "radio", "file", "color", "range"].includes(type);
    }

    function focusSafely(element) {
        if (!element || typeof element.focus !== "function") return;
        try {
            element.focus({ preventScroll: true });
        } catch (_) {
            try {
                element.focus();
            } catch (_) {}
        }
    }

    window.playAlert = function(message) {
        return new Promise(function(resolve) {
            const { root, box, message: messageEl, actions } = getDialogParts();
            const previouslyFocused = document.activeElement;
            const openedFromTextEntry = isTextEntryElement(previouslyFocused);
            if (openedFromTextEntry) {
                previouslyFocused.blur();
            }

            messageEl.textContent = String(message || "");
            actions.innerHTML = "";

            const close = function() {
                root.style.display = "none";
                document.body.style.overflow = "";
                if (previouslyFocused && typeof previouslyFocused.focus === "function" && !isTextEntryElement(previouslyFocused)) {
                    previouslyFocused.focus();
                }
                resolve();
            };

            const okButton = createButton("Entendi", "play-dialogs-btn-primary", close);
            actions.appendChild(okButton);

            root.onclick = function(event) {
                if (event.target === root) close();
            };

            root.style.display = "flex";
            document.body.style.overflow = "hidden";
            window.setTimeout(function() {
                focusSafely(openedFromTextEntry ? box : okButton);
            }, 0);
        });
    };

    window.playConfirm = function(message, options) {
        return new Promise(function(resolve) {
            const { root, box, message: messageEl, actions } = getDialogParts();
            const previouslyFocused = document.activeElement;
            const openedFromTextEntry = isTextEntryElement(previouslyFocused);
            const settings = options || {};
            const hasOwn = Object.prototype.hasOwnProperty;
            const secondaryLabel = settings.secondaryLabel || settings.cancelLabel || "Cancelar";
            const primaryLabel = settings.primaryLabel || settings.confirmLabel || "Continuar";
            const secondaryResult = hasOwn.call(settings, "secondaryResult") ? settings.secondaryResult : false;
            const primaryResult = hasOwn.call(settings, "primaryResult") ? settings.primaryResult : true;
            const overlayResult = hasOwn.call(settings, "overlayResult") ? settings.overlayResult : false;
            if (openedFromTextEntry) {
                previouslyFocused.blur();
            }

            messageEl.textContent = String(message || "");
            actions.innerHTML = "";

            const close = function(result) {
                root.style.display = "none";
                document.body.style.overflow = "";
                if (previouslyFocused && typeof previouslyFocused.focus === "function" && !isTextEntryElement(previouslyFocused)) {
                    previouslyFocused.focus();
                }
                resolve(result);
            };

            const cancelButton = createButton(secondaryLabel, "play-dialogs-btn-secondary", function() {
                close(secondaryResult);
            });
            const confirmButton = createButton(primaryLabel, "play-dialogs-btn-primary", function() {
                close(primaryResult);
            });

            actions.appendChild(cancelButton);
            actions.appendChild(confirmButton);

            root.onclick = function(event) {
                if (event.target === root) close(overlayResult);
            };

            root.style.display = "flex";
            document.body.style.overflow = "hidden";
            window.setTimeout(function() {
                focusSafely(openedFromTextEntry ? box : confirmButton);
            }, 0);
        });
    };

    window.alert = function(message) {
        return window.playAlert(message);
    };
})();
