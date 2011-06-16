"""
=== SODI container objects prototype ===
version 1

All data is stored as text in json format. Helper functions and
classes are provided in this module together with usage documentation. See
also func calling demo at the end of this file.

*** Compatible with dbdict (dict with items accessibe via attributes)

>>> x = Struct()
>>> x.field1 = 'test'
>>> x['field1']
'test'


*** Object can automatically be built from json, using class method from_json

>>> x = Struct.from_json('{"field2":42, "field3":{"field4":"foo"}}')
>>> x.field2
42


*** Struct data can be dumped to json using method dump_json

>>> x.dump_json()
'{"field2":42,"field3":{"field4":"foo"}}'


*** Sub dicts in json are automatically converted to Struct objects on creation
(note that sting values are stored as unicode, that is the standard behaviour of
json)

>>> x.field3.field4 == 'foo'
True

*** Typed structs can be defined using simple class defination syntax. All fields
are instances of Field. Field types are given as arguments into Field initializator.
When no field type is given, no automatic type conversion takes place

>>> class X(Struct):
...     name = Field(str)
...     age = Field(int, default=42)
...     blah = Field()
>>> x = X()
>>> x.name
''
>>> x.age
42
>>> type(x.blah)
<type 'NoneType'>


**** Structs are inheritable

>>> class Y(X):
...     some_value = Field(float)
>>> y = Y()
>>> y.age
42
>>> y.some_value
0.0


*** New combined structs can be built by adding up base structs

>>> class A(Struct):
...     name = Field(str, default = 'nobody')
>>> class B(Struct):
...     age = Field(int, default = 11)
>>> AB = A + B
>>> ab=AB()
>>> ab.name
'nobody'
>>> ab['age']
11


*** Special "typed" list bulder list_of is introduced with method new for creating
(and adding) new typed items

>>> class Item(Struct):
...     key = Field(str)
...     val = Field(int)
>>> class ItemContainer(Struct):
...     name = Field(str)
...     itemlist = Field(list_of(Item))
>>> x = ItemContainer(name = 'test')
>>> item = x.itemlist.new()
>>> item.key
''
>>> item.val
0
>>> item.key = 'test-test'
>>> item.val = 42
>>> x.itemlist[0].key
'test-test'
>>> item = x.itemlist.new(key=2, val='100')
>>> x.itemlist[1].val
100
>>> item.key
'2'


*** Helper function for struct building: build_struct. base structs inherit from
can be given as args and fields as keyword args. If ordinary type is used in 
place of field, Field object is built automatically, using type as arg

>>> Foo = build_struct(name = Field(str, default='nobody'), age = int)
>>> foo = Foo()
>>> foo.name
'nobody'
>>> foo.age
0
>>> Foo1 = build_struct(Foo, data = build_struct(lines=list_of(build_struct(nr=int, line=str))))
>>> foo1 = Foo1()
>>> line = foo1.data.lines.new(nr=1, line='line1')
>>> foo1['data'].lines[0].line
'line1'
>>> foo1.name
'nobody'
"""

#from pdb import set_trace

__all__ = ['loads', 'dumps', 'Struct', 'build_struct', 'Field', 'list_of']

#===============================================================================
# Portable JSON import
#===============================================================================

import sys

# python2.5 'json' is crap, try simplejson first
try:
    import simplejson as json
except ImportError:
    # python tries to do relative import first, work around it
    import sys
    __import__('json', level=0)
    json = sys.modules['json']

if hasattr(json, 'dumps'):
    dumps = json.dumps
    loads = json.loads
else:
    # python2.5-json
    dumps = getattr(json, 'write')
    loads = getattr(json, 'read')


#===============================================================================
# Inherited stuff (defined here now to eliminate dependencies)
#===============================================================================


class dbdict(dict):
    """use dbdict from skytools instead for maximum compatibility"""      
    def __getattr__(self, k):
        return self[k]

    def __setattr__(self, k, v):
        self[k] = v

    def __delattr__(self, k):
        del self[k]


#===============================================================================
# Magic that makes everything work
#===============================================================================


class _MetaStruct(type):
    """Builds struct classes with field initialization and type conversion
    based on descriptions (Struct and its subclasses)
    """
    def __new__(cls, name, bases, attrs):        
        _fields = {}
        _attrs = {}
        for attrname, attr in attrs.items():
            if isinstance(attr, Field):
                _fields[attrname] = attr
            #elif not attrname.startswith('__'):
            #    _fields[attrname] = Field(attr) 
            else:
                _attrs[attrname] = attr
                
        def __setitem__(self, k, v):
            if k in _fields:
                v = _fields[k](v)
            dbdict.__setitem__(self, k, v)
            
        def __init__(self, *p, **kw):
            for base in bases:
                base.__init__(self, *p, **kw)
                    
            for fieldname, field in _fields.iteritems():
                self[fieldname] = field(self.get(fieldname))
                
            for key, val in self.iteritems():
                if not key in _fields:
                    if type(val) == dict:
                        self[key] = Struct(val)
                    elif type(val) == list:
                        self[key] = list_of(Struct)(val)                    
                
        _attrs.update({'__init__': __init__, '__setitem__':__setitem__})
        return super(_MetaStruct, cls).__new__(cls, name, bases, _attrs)

    def __init__(self, name, bases, attrs):
        super(_MetaStruct, self).__init__(name, bases, attrs)
        
    def __add__(self, other):
        return type('CombinedStruct', (self, other), {})


#===============================================================================
# Building blocks
#===============================================================================


class Field(object):
    """Struct field definition"""
    def __init__(self, type = None, default = None):
        self.type = type
        self.default = default

    def __call__(self, value = None):
        _value = value
        _default = self.default
        if _value is None:
            if callable(_default):
                _default = _default()
            _value = _default
        if self.type is not None and type(_value) != self.type:
            _args = [_value] if _value else [] 
            _value = self.type(*_args)
        return _value


class Struct(dbdict):
    """Base class. All sutructure components must inherit or
    instantiate Struct. Magic happens in metaclass.
    """
    __metaclass__ = _MetaStruct

    @classmethod
    def from_json(cls, jsonstr):
        """creates object from json string"""
        return cls(json.loads(jsonstr))

    def dump_json(self):
        """dumps object to json string"""
        return json.dumps(self)
    
    def getas(self, name, cast = None, default = None):
        """get value by name with optional casting and default"""
        value = self.get(name, default)
        if cast:
            value = cast(value)
        return value


#===============================================================================
# Helpers and builders
#===============================================================================


def list_of(itemtype):
    """typed list handler builder"""
    class ListHandler(list):
        def __init__(self, *p):
            list.__init__(self, *p)
            for i, item in enumerate(self):
                if not isinstance(item, itemtype):
                    self[i] = itemtype(item)
            
        def new(self, *p, **kw):
            item = itemtype(*p, **kw)
            self.append(item)
            return item                
    return ListHandler


def build_struct(*p, **kw):
    """Struct subclass building helper"""
    return type('BuildStruct',
        p or (Struct,),
        dict((k, v if isinstance(v, Field) else Field(v))
            for k,v in kw.iteritems()))


#===============================================================================
# DEMO
#===============================================================================


def func_call_example():
    """Funcion call is simulated, using argument defining, json string creation
    and result processing
    """
    class Context(Struct):
        username = Field(str, default = lambda: 'egon')        
        
    class AbstractCall(Struct):
        func = Field(str)
        context = Field(Context) 
        
    class AbstractResult(Struct):
        code = Field(int, default = 200)
        msg = Field(str, default = 'OK')
        
    # testcall with json as arg and result
    def func_call(json_arg):
        call = AbstractCall.from_json(json_arg)
        assert call.func == 'public.test'
        assert call.context.username == 'egon'
        assert call.params.hostname == 'dub-db1'

        Row = build_struct(id = Field(int), value = Field(str))
        Result = build_struct(AbstractResult,
                        rows = Field(list_of(Row)),
                        created = Field(str, default = lambda: __import__('datetime').datetime.now()))

        result = Result()        
        result.rows.new(id = 1, value = 'a')
        row = result.rows.new()
        row.id = 2
        row.value = 'b'
        row = result.rows.new(id = 3)
        row.value = 'c'
        result.rows.new({'id':4, 'value':'d'})
        return result.dump_json()
    
    # define concrete func call    
    Params = build_struct(hostname = Field(str), ip = Field(str))
    Call = build_struct(AbstractCall, params = Field(Params))
    
    call = Call()
    call.func = 'public.test'
    call.params.hostname = 'dub-db1'
    call.params.ip ='192.168.1.1'
    
    # create json, call and parse result json
    json_call_str = call.dump_json()
    print json_call_str
    json_result_str = func_call(json_call_str)    
    print json_result_str
    result = AbstractResult.from_json(json_result_str)
    
    assert result.code == 200
    assert result.msg == 'OK'
    assert len(result.rows) == 4
    assert result.rows[0].id == 1
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()    
    func_call_example()

