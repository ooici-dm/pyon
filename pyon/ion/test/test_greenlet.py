#!/usr/bin/env python
'''
@author Luke Campbel <LCampbell@ASAScience.com>
@file 
@date 03/30/12 14:47
@description DESCRIPTION
'''
from gevent.greenlet import Greenlet
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

@attr('INT')
class TestGreenlet(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

        self.container.spawn_process(module='examples.service.bad_service',cls='BadService')


    def test_timeout(self):
        import time
        import gevent
        from csleep import csleep
        g = Greenlet(csleep,5)
        then = time.time()
        g.start()
        gevent.sleep(1)
        now = time.time()
        self.assertTrue((now - then) < 2, 'The greenlets were blocked by the C-lib call.')