
import bisect

import msgpack
import numpy
import hashlib

class SortedDict(dict):
    """
    A sorted dictionary

    @todo The insert operation is slow - would it be faster to sort on the way out every time?
    What about updating when ever the sorted content is needed, but only doing the sort on the way out?
    """

    CMP = None

    def __init__(self,*args,**kwargs):

        super(SortedDict, self).__init__(*args,**kwargs)

        self._sort = super(SortedDict, self).keys()
        self._sort.sort(cmp=SortedDict.CMP)


    def __iter__(self):
        """
        Sort the keys on the way out
        """
        for key in self._sort:
            yield key


    def iteritems(self):
        for key in self._sort:
            yield key, self[key]

    def itervalues(self):
        for key in self._sort:
            yield self[key]

    def iterkeys(self):
        for key in self._sort:
            yield key


    def __setitem__(self, key, value):

        super(SortedDict, self).__setitem__(key, value)

        bisect.insort_left(self._sort,key)



class SortedDict2(object):
    """
    A sorted dictionary

    @todo The insert operation is slow - would it be faster to sort on the way out every time?
    What about updating when ever the sorted content is needed, but only doing the sort on the way out?
    """

    CMP = None

    def __init__(self,*args,**kwargs):


        self._d=dict(*args,**kwargs)

        self._sort = self._d.keys()
        self._sort.sort(cmp=SortedDict2.CMP)


    def __iter__(self):
        """
        Sort the keys on the way out
        """
        for key in self._sort:
            yield key

    def keys(self):
        return self._sort

    def iteritems(self):
        for key in self._sort:
            yield key, self._d[key]

    def itervalues(self):
        for key in self._sort:
            yield self._d[key]

    def iterkeys(self):
        for key in self._sort:
            yield key


    def __setitem__(self, key, value):

        self._d[key] = value

        bisect.insort_left(self._sort,key)


    def __getitem__(self, key):

        return self._d[key]


if __name__ == '__main__':


    def decode_ion( obj):
        """
        This method is only called on dictionary objects... turn them all into SortDict...
        """

        # Same as interceptor encode...
        if "__set__" in obj:
            return set(obj['tuple'])

        elif "__list__" in obj:
            return list(obj['tuple'])

        elif "__ion_array__" in obj:
            # Shape is currently implicit because tolist encoding makes a list of lists for a 2d array.
            return numpy.array(obj['content'],dtype=numpy.dtype(obj['header']['type']))

        elif '__complex__' in obj:
            return complex(obj['real'], obj['imag'])
            ## Always return object

        # Now turn everything else into SortDict
        elif '__sorted_dict__' in obj:
            return SortedDict2(obj['list'])

        return obj

    def encode_ion( obj):
        """
        MsgPack object hook to encode any ion object as part of the message pack walk rather than implementing it again in
        pyon
        """

        if isinstance(obj, SortedDict2):
            return {'__sorted_dict__':True,'list':[tup for tup in obj.iteritems()]}

        if isinstance(obj, list):
            return {"__list__":True, 'tuple':tuple(obj)}

        if isinstance(obj, set):
            return {"__set__":True, 'tuple':tuple(obj)}

        if isinstance(obj, numpy.ndarray):
            if obj.ndim == 0:
                raise ValueError('Can not encode a numpy array with rank 0')
            return {"header":{"type":str(obj.dtype),"nd":obj.ndim,"shape":obj.shape},"content":obj.tolist(),"__ion_array__":True}

        if isinstance(obj, complex):
            return {'__complex__': True, 'real': obj.real, 'imag': obj.imag}

        if isinstance(obj, (numpy.float, numpy.float16, numpy.float32, numpy.float64)):
            raise ValueError('Can not encode numpy scalars!')


        # Must raise type error to avoid recursive failure
        raise TypeError('Unknown type "%s" in user specified encoder: "%s"' % (str(type(obj)), str(obj)))




    sd1 = SortedDict2(foo='bar',baz=5)
    sd1['john']='joe'
    sd1[5]='val'
    sd1['key']=[1,2,3]

    for i in xrange(100):

        sd1['stuff %d' % i ] = i**2



    # Don't need to modify encode at all!
    packed = msgpack.packb(sd1,default=encode_ion)


    sd2 = msgpack.unpackb(packed, object_hook=decode_ion, use_list=1)

    print "Sorted Dictionary out equal? '%s'" % (sd2 == sd1)

    sha1 = hashlib.sha1(packed).hexdigest().upper()


    new_sha = hashlib.sha1(msgpack.packb(sd2,default=encode_ion)).hexdigest().upper()

    print "Hash of encoded decode encode is equal? '%s'" % (new_sha == sha1)

    for i in xrange(1000):

        sd_new = SortedDict2(sd1._d)
        new_sha = hashlib.sha1(msgpack.packb(sd_new,default=encode_ion)).hexdigest().upper()

        if new_sha != sha1:
            print 'Test Failed!'
            break

    else:
        print 'Random Keys Test 1 succeeded!'



    for i in xrange(1000):

        # make sure the order in which the dictionary is built is random
        sd_new = SortedDict2()
        keyset = set(sd1.keys())
        for key in keyset:
            sd_new[key] = sd1[key]

        new_sha = hashlib.sha1(msgpack.packb(sd_new,default=encode_ion)).hexdigest().upper()

        if new_sha != sha1:
            print 'Test Failed!'
            break

    else:
        print 'Random Keys Test 2 succeeded!'



