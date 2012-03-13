import imp
import os
import sys




_this_dir = os.path.dirname(os.path.realpath(__file__))
_this_module_name = globals()['__name__']
_this_module = sys.modules[_this_module_name]
assert(globals() is _this_module.__dict__)

_sources = [_f for _f in os.listdir(_this_dir) \
            if _f.endswith('.py') and _f != '__init__.py']
for _source in _sources:
    _name = _source[:-3]
    _fp, _path, _desc = imp.find_module(_name, [_this_dir, ])
    try:
        _m = imp.load_module(_this_module_name + '.' + _name, _fp, _path, _desc)
    finally:
        if(_fp):
            _fp.close()
    setattr(_this_module, _name, _m)



