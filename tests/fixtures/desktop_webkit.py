"""Native WebKitGTK embedding, including its separate accessibility process."""
import gi

gi.require_version('Gtk', '3.0')
gi.require_version('WebKit2', '4.1')
from gi.repository import Gtk, WebKit2

window = Gtk.Window(title='WebKit input fixture')
window.set_default_size(600, 720)
window.move(20, 20)
window.connect('destroy', Gtk.main_quit)
view = WebKit2.WebView()
view.connect('notify::title', lambda view, _: window.set_title(view.get_title() or 'WebKit input fixture'))
view.load_html('''<!doctype html><meta charset="utf-8"><title>WebKit input fixture</title>
<style>body{margin:0}button{height:60px}textarea{box-sizing:border-box;width:100%;height:80vh;font-size:24px}</style>
<button autofocus>Tap the editor below to focus it</button>
<textarea placeholder="点击并输入中文" oninput="document.title='Input:'+this.value"></textarea>''', 'file:///workspace/')
window.add(view)
window.show_all()
view.grab_focus()
Gtk.main()
