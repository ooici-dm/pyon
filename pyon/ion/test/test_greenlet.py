#!/usr/bin/env python
'''
@author Luke Campbel <LCampbell@ASAScience.com>
@file 
@date 03/30/12 14:47
@description DESCRIPTION
'''

from pyon.util.unit_test import PyonTestCase
from gevent.greenlet import Greenlet
from nose.plugins.attrib import attr

@attr('UNIT')
class TestGreenlet(PyonTestCase):


    def test_timeout(self):
        import time
        import gevent
        from csleep import csleep
        g = Greenlet(csleep,5)
        then = time.time()
        g.start()
        gevent.sleep(1)
        now = time.time()
        self.assertFalse((now - then) < 2, 'The greenlets were not blocked by a the libc blocking call.')