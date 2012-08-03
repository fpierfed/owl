#!/usr/bin/env python
import ctypes
import ctypes.util
import os
import sys




try:
    graphlib = ctypes.cdll.LoadLibrary(ctypes.util.find_library('gvc'))
except:
    graphlib = ctypes.cdll.LoadLibrary('/usr/local/lib/libgvc.dylib')


f = open('bcw.dot', 'r')
dot = f.read()
f.close()

cLength = ctypes.c_int(1)
cSvg = ctypes.pointer(ctypes.create_string_buffer(4096))

cFormat = ctypes.create_string_buffer('svg')
cLayout = ctypes.create_string_buffer('dot')
cDot = ctypes.create_string_buffer(dot)

gvc = graphlib.gvContext()
g = graphlib.agmemread(ctypes.byref(cDot))

graphlib.gvLayout(gvc, g, ctypes.byref(cLayout))
graphlib.gvRenderData(gvc, g, ctypes.byref(cFormat), ctypes.byref(cSvg), ctypes.byref(cLength))

graphlib.gvFreeLayout(gvc, g)
graphlib.agclose(g)
graphlib.gvFreeContext(gvc)

svg = ctypes.string_at(cSvg)
print(svg)


