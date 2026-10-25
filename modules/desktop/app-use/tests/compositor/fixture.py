"""Native GTK/Wayland fixture; input effects are observable in fixture-state.json."""
import json
import os
from pathlib import Path
import gi
gi.require_version("Gtk", "4.0")
from gi.repository import Gtk, GLib, Gdk

state = {"clicks": 0, "text": "", "motion": 0, "scrolls": 0, "scroll_y": 0, "frames": 0}


def save():
    path = Path("/out/fixture-state.json")
    path.with_suffix(".tmp").write_text(json.dumps(state))
    path.with_suffix(".tmp").replace(path)


def activate(app):
    window = Gtk.ApplicationWindow(application=app, title="Sinomo Wayland fixture")
    window.set_default_size(1280, 800)
    window.fullscreen()
    box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=16)
    box.set_margin_top(40)
    box.set_margin_start(40)
    box.set_margin_end(40)
    label = Gtk.Label(label="Sinomo Wayland fixture")
    box.append(label)
    button = Gtk.Button(label="Count native click")
    button.set_size_request(-1, 70)

    def clicked(_):
        state["clicks"] += 1
        label.set_label(f"Native clicks: {state['clicks']}")
        save()
    button.connect("clicked", clicked)
    box.append(button)
    entry = Gtk.Entry(placeholder_text="Native text input")
    entry.set_size_request(-1, 60)
    entry.connect("changed", lambda widget: (state.update(text=widget.get_text()), save()))
    box.append(entry)
    canvas = Gtk.DrawingArea()
    canvas.set_size_request(-1, 400)

    def draw(_, ctx, width, height):
        ctx.set_source_rgb(.04, .3, .8)
        ctx.paint()
        ctx.set_source_rgb(1, .9, 0)
        ctx.rectangle((state["frames"] % 12) * 50, 20, 120, 120)
        ctx.fill()
    canvas.set_draw_func(draw)
    motion = Gtk.EventControllerMotion()
    def moved(_, x, y):
        state["motion"] += 1
        state["last_motion"] = [x, y]
        save()
    motion.connect("motion", moved)
    canvas.add_controller(motion)
    scroll = Gtk.EventControllerScroll.new(Gtk.EventControllerScrollFlags.BOTH_AXES)
    def scrolled(_, dx, dy):
        state["scrolls"] += 1
        state["scroll_y"] += dy
        save()
        return True
    scroll.connect("scroll", scrolled)
    canvas.add_controller(scroll)
    box.append(canvas)
    window.set_child(box)
    window.present()
    state["display"] = type(Gdk.Display.get_default()).__name__
    state["wayland_display"] = os.environ.get("WAYLAND_DISPLAY")
    save()

    def animate():
        state["frames"] += 1
        state["active"] = window.is_active()
        state["size"] = [window.get_width(), window.get_height()]
        for name, widget in [("button", button), ("entry", entry), ("canvas", canvas)]:
            ok, rect = widget.compute_bounds(window)
            if ok:
                state[name] = [rect.get_x(), rect.get_y(), rect.get_width(), rect.get_height()]
        save()
        canvas.queue_draw()
        return True
    GLib.timeout_add(200, animate)


app = Gtk.Application(application_id="net.sinomo.AppUseFixture")
app.connect("activate", activate)
app.run(None)
