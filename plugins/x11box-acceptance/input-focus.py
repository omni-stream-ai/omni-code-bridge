"""Read the foreground native application's editable focus through AT-SPI."""

import os
from pathlib import Path
import shlex
import signal
import subprocess


def focused_input():
    # docker exec does not inherit the isolated display's session environment.
    state = Path(os.environ.get("X11BOX_STATE_DIR", "/var/lib/x11box/state/x11box"))
    for line in (state / "env").read_text().splitlines():
        if not line.startswith("export "):
            continue
        key, value = line[7:].split("=", 1)
        if key in {"DISPLAY", "XAUTHORITY", "HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME"}:
            words = shlex.split(value)
            os.environ[key] = words[0] if words else ""

    # An unfocused window can retain its internal FOCUSED state. Never use a
    # background application's input to open the phone keyboard.
    pid = int(subprocess.check_output(
        ["xdotool", "getactivewindow", "getwindowpid"],
        timeout=0.3, stderr=subprocess.DEVNULL,
    ).strip())

    import gi
    gi.require_version("Atspi", "2.0")
    from gi.repository import Atspi

    Atspi.set_timeout(150, 150)
    desktop = Atspi.get_desktop(0)
    for index in range(min(desktop.get_child_count(), 128)):
        app = desktop.get_child_at_index(index)
        if app.get_process_id() != pid:
            continue
        for child in range(min(app.get_child_count(), 128)):
            window = app.get_child_at_index(child)
            if not window.get_state_set().contains(Atspi.StateType.ACTIVE):
                continue
            # Collection.GetMatches stops at embedded providers. Walking the
            # active window also reaches Tauri's separate WebKit web process.
            pending = [window]
            remaining = 512
            while pending and remaining:
                node = pending.pop()
                remaining -= 1
                states = node.get_state_set()
                if states.contains(Atspi.StateType.FOCUSED):
                    if all(states.contains(state) for state in (
                        Atspi.StateType.EDITABLE, Atspi.StateType.ENABLED, Atspi.StateType.SENSITIVE,
                    )):
                        return True
                count = node.get_child_count()
                if count > remaining:
                    return None
                pending.extend(node.get_child_at_index(i) for i in reversed(range(count)))
            if not pending:
                return False
    return None


if __name__ == "__main__":
    # A broken accessibility provider must not stall the keyboard focus poll.
    signal.setitimer(signal.ITIMER_REAL, 0.9)
    try:
        focused = focused_input()
    except Exception:
        focused = None
    print("OMNI_INPUT_" + ({True: "FOCUSED", False: "BLURRED"}.get(focused, "UNSUPPORTED")))
