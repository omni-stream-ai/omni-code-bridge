"""A native X11 fixture with visible order state and no web dependencies."""
import ctypes as c
import sys

x = c.CDLL('libX11.so.6')
ptr = c.c_void_p
wid = c.c_ulong

class Font(c.Structure):
    _fields_ = [('ext_data', ptr), ('fid', wid)]

class ButtonEvent(c.Structure):
    _fields_ = [('type', c.c_int), ('serial', c.c_ulong), ('send_event', c.c_int),
                ('display', ptr), ('window', wid), ('root', wid), ('subwindow', wid),
                ('time', c.c_ulong), ('x', c.c_int), ('y', c.c_int)]

def signature(name, args, result=c.c_int):
    function = getattr(x, name)
    function.argtypes = args
    function.restype = result
    return function

signature('XOpenDisplay', [c.c_char_p], ptr)
signature('XDefaultScreen', [ptr])
signature('XRootWindow', [ptr, c.c_int], wid)
signature('XCreateSimpleWindow', [ptr, wid, c.c_int, c.c_int, c.c_uint, c.c_uint, c.c_uint, c.c_ulong, c.c_ulong], wid)
signature('XStoreName', [ptr, wid, c.c_char_p])
signature('XSelectInput', [ptr, wid, c.c_long])
signature('XMapWindow', [ptr, wid])
signature('XNextEvent', [ptr, ptr])
signature('XLookupKeysym', [ptr, c.c_int], c.c_ulong)
signature('XFlush', [ptr])
signature('XDefaultGC', [ptr, c.c_int], ptr)
signature('XLoadQueryFont', [ptr, c.c_char_p], c.POINTER(Font))
signature('XSetFont', [ptr, ptr, wid])
signature('XSetForeground', [ptr, ptr, c.c_ulong])
signature('XFillRectangle', [ptr, wid, ptr, c.c_int, c.c_int, c.c_uint, c.c_uint])
signature('XDrawString', [ptr, wid, ptr, c.c_int, c.c_int, c.c_char_p, c.c_int])
signature('XClearWindow', [ptr, wid])

display = x.XOpenDisplay(None)
assert display, 'X display unavailable'
screen = x.XDefaultScreen(display)
root = x.XRootWindow(display, screen)
window = x.XCreateSimpleWindow(display, root, 20, 20, 390, 600, 0, 0, 0xf8fafc)
gc = x.XDefaultGC(display, screen)
for name in [b'10x20', b'9x15', b'fixed']:
    font = x.XLoadQueryFont(display, name)
    if font:
        x.XSetFont(display, gc, font.contents.fid)
        break

title = sys.argv[sys.argv.index('--title') + 1]
confirmed = False

def text(left, top, value, color=0x172b35):
    encoded = value.encode()
    x.XSetForeground(display, gc, color)
    x.XDrawString(display, window, gc, left, top, encoded, len(encoded))

def rectangle(left, top, width, height, color):
    x.XSetForeground(display, gc, color)
    x.XFillRectangle(display, window, gc, left, top, width, height)

def draw():
    x.XClearWindow(display, window)
    text(24, 48, 'NATIVE ORDER DEMO')
    text(24, 102, 'Order #A100')
    text(24, 132, 'Total: $128.00')
    rectangle(24, 164, 340, 76, 0xa7f3d0 if confirmed else 0xffe0a3)
    text(48, 210, 'CONFIRMED' if confirmed else 'PENDING')
    text(24, 278, 'Order confirmed.' if confirmed else 'Please confirm this order.')
    rectangle(24, 308, 340, 64, 0x176b60)
    text(60, 348, 'CONFIRM ORDER', 0xffffff)
    text(24, 428, 'Enter = confirm')
    text(24, 458, 'R = reset for another trial')
    x.XFlush(display)

def update(value):
    global confirmed
    confirmed = value
    x.XStoreName(display, window, (title + (' - Confirmed' if confirmed else ' - Pending')).encode())
    draw()

x.XStoreName(display, window, (title + ' - Pending').encode())
x.XSelectInput(display, window, 1 | 4 | (1 << 15) | (1 << 17))
x.XMapWindow(display, window)
x.XFlush(display)
event = (c.c_long * 24)()
while True:
    x.XNextEvent(display, c.byref(event))
    kind = c.cast(c.byref(event), c.POINTER(c.c_int))[0]
    if kind == 2:
        key = x.XLookupKeysym(c.byref(event), 0)
        if key == 0xff0d:
            update(True)
        elif key in (ord('r'), ord('R')):
            update(False)
    elif kind == 4:
        button = c.cast(c.byref(event), c.POINTER(ButtonEvent)).contents
        if 24 <= button.x <= 364 and 308 <= button.y <= 372:
            update(True)
    elif kind in (12, 22):
        draw()
