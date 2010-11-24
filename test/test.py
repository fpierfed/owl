#!/usr/bin/env python
# Copyright (C) 2010 Association of Universities for Research in Astronomy(AURA)
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
# 
#     2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
# 
#     3. The name of AURA and its representatives may not be used to
#       endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY AURA ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AURA BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
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


