"""Inspect or click real accessibility controls for local test setup/approval."""
import json
import sys
import gi
gi.require_version("Atspi", "2.0")
from gi.repository import Atspi

nodes = []


def visit(node, depth=0):
    if depth > 30 or len(nodes) > 4096:
        return
    try:
        role, name = node.get_role_name(), node.get_name()
        if name:
            bounds = None
            try:
                rect = node.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
                bounds = [rect.x, rect.y, rect.width, rect.height]
            except Exception:
                pass
            states = node.get_state_set()
            nodes.append((node, {"name": name, "role": role, "bounds": bounds,
                                 "checked": states.contains(Atspi.StateType.CHECKED),
                                 "enabled": states.contains(Atspi.StateType.ENABLED)}))
        for i in range(node.get_child_count()):
            visit(node.get_child_at_index(i), depth + 1)
    except Exception:
        pass


visit(Atspi.get_desktop(0))
if len(sys.argv) > 1:
    matches = [(node, info) for node, info in nodes if info["name"] == sys.argv[1]]
    for node, info in matches:
        try:
            actions = node.get_action_iface()
            for i in range(actions.get_n_actions()):
                if actions.get_action_name(i).casefold() in ("click", "press", "activate", "toggle"):
                    assert actions.do_action(i)
                    print(json.dumps(info))
                    raise SystemExit(0)
        except NotImplementedError:
            pass
    raise SystemExit("No actionable control named " + sys.argv[1])
print(json.dumps([info for _, info in nodes], indent=2))
