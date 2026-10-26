"""Real GTK text editing, without a browser or CDP focus signal."""
import gi

gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

window = Gtk.Window(title='Native input fixture')
window.set_default_size(600, 720)
window.move(20, 20)
window.connect('destroy', Gtk.main_quit)
editor = Gtk.TextView()
editor.set_wrap_mode(Gtk.WrapMode.WORD_CHAR)
editor.set_left_margin(20)
editor.set_top_margin(20)
buffer = editor.get_buffer()

def changed(buffer):
    window.set_title('Input:' + buffer.get_text(buffer.get_start_iter(), buffer.get_end_iter(), True))

buffer.connect('changed', changed)
layout = Gtk.Box(orientation=Gtk.Orientation.VERTICAL)
initial_focus = Gtk.Button(label='Tap the editor below to focus it')
initial_focus.set_size_request(-1, 60)
layout.pack_start(initial_focus, False, False, 0)
layout.pack_start(editor, True, True, 0)
window.add(layout)
window.show_all()
initial_focus.grab_focus()
Gtk.main()
