#!/usr/bin/env python
from pyon.core.bootstrap import get_sys_name

__author__ = 'Dave Foster <dfoster@asascience.com>'

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr
from examples.service.hello_service import HelloService
from interface.services.examples.hello.ihello_service import HelloServiceClient
from pyon.net.endpoint import RPCServer
from pyon.util.async import spawn
import unittest
from pyon.ion.exchange import ExchangeManager, ION_ROOT_XS, ExchangeNameProcess, ExchangeSpace, ExchangePoint, ExchangeNameService, ExchangeName, ExchangeNameQueue
from mock import Mock, sentinel
from pyon.net.transport import BaseTransport

@attr('UNIT', group='exchange')
class TestExchangeObjects(IonUnitTestCase):
    def setUp(self):
        self.ex_manager = ExchangeManager(Mock())
        self.ex_manager._transport  = Mock(BaseTransport)
        self.ex_manager._client     = Mock()
        # all exchange level operations are patched out via the _transport

    def test_exchange_by_name(self):
        # defaults: Root XS, no XNs
        self.assertIn(ION_ROOT_XS, self.ex_manager.xs_by_name)
        self.assertIn(self.ex_manager.default_xs, self.ex_manager.xs_by_name.itervalues())
        self.assertEquals(len(self.ex_manager.xn_by_name), 0)

        # create another XS
        xs = self.ex_manager.create_xs('exchange')
        self.assertIn('exchange', self.ex_manager.xs_by_name)
        self.assertIn(xs, self.ex_manager.xs_by_name.values())
        self.assertEquals(len(self.ex_manager.xn_by_name), 0)

        # now create some XNs underneath default exchange
        xn1 = self.ex_manager.create_xn_process('xn1')
        self.assertEquals(xn1._xs, self.ex_manager.default_xs)
        self.assertIn('xn1', self.ex_manager.xn_by_name)
        self.assertIn(xn1, self.ex_manager.xn_by_name.values())
        self.assertEquals(xn1, self.ex_manager.xn_by_name['xn1'])
        self.assertIsInstance(xn1, ExchangeNameProcess)

        self.assertEquals({ION_ROOT_XS:[xn1]}, self.ex_manager.xn_by_xs)

        xn2 = self.ex_manager.create_xn_service('xn2')
        self.assertIn('xn2', self.ex_manager.xn_by_name)
        self.assertIn(xn2, self.ex_manager.xn_by_xs[ION_ROOT_XS])
        self.assertEquals(xn2.xn_type, 'XN_SERVICE')

        # create one under our second xn3
        xn3 = self.ex_manager.create_xn_queue('xn3', xs)
        self.assertIn('xn3', self.ex_manager.xn_by_name)
        self.assertIn(xn3, self.ex_manager.xn_by_xs['exchange'])
        self.assertNotIn(xn3, self.ex_manager.xn_by_xs[ION_ROOT_XS])

    def test_create_xs(self):
        xs      = self.ex_manager.create_xs(sentinel.xs)
        exstr   = '%s.ion.xs.%s' % (get_sys_name(), str(sentinel.xs))     # what we expect the exchange property to return

        self.assertEquals(xs._exchange, sentinel.xs)
        self.assertEquals(xs.exchange, exstr)
        self.assertEquals(xs.queue, None)
        self.assertEquals(xs.binding, None)

        self.assertEquals(xs._xs_exchange_type, 'topic')
        self.assertEquals(xs._xs_durable, False)
        self.assertEquals(xs._xs_auto_delete, True)

        # should be in our map too
        self.assertIn(sentinel.xs, self.ex_manager.xs_by_name)
        self.assertEquals(self.ex_manager.xs_by_name[sentinel.xs], xs)

        # should've tried to declare
        self.ex_manager._transport.declare_exchange_impl.assert_called_once_with(self.ex_manager._client, exstr, auto_delete=True, durable=False, exchange_type='topic')

    def test_create_xs_with_params(self):
        xs      = self.ex_manager.create_xs(sentinel.xs, exchange_type=sentinel.ex_type, durable=True)
        exstr   = '%s.ion.xs.%s' % (get_sys_name(), str(sentinel.xs))     # what we expect the exchange property to return

        self.assertEquals(xs._xs_durable, True)
        self.assertEquals(xs._xs_exchange_type, sentinel.ex_type)

        # declaration?
        self.ex_manager._transport.declare_exchange_impl.assert_called_once_with(self.ex_manager._client, exstr, auto_delete=True, durable=True, exchange_type=sentinel.ex_type)

    def test_delete_xs(self):
        # need an XS first
        xs      = self.ex_manager.create_xs(sentinel.delete_me)
        exstr   = '%s.ion.xs.%s' % (get_sys_name(), str(sentinel.delete_me))     # what we expect the exchange property to return

        self.assertIn(sentinel.delete_me, self.ex_manager.xs_by_name)

        self.ex_manager.delete_xs(xs)

        self.assertNotIn(sentinel.delete_me, self.ex_manager.xs_by_name)

        # call to broker
        self.ex_manager._transport.delete_exchange_impl.assert_called_once_with(self.ex_manager._client, exstr)

    def test_delete_xs_without_creating_it_first(self):
        xsmock = Mock(ExchangeSpace)
        xsmock._exchange = sentinel.fake

        self.assertRaises(KeyError, self.ex_manager.delete_xs, xsmock)

    def test_create_xp(self):
        xp      = self.ex_manager.create_xp(sentinel.xp)
        exstr   = "%s.ion.xs.%s.xp.%s" % (get_sys_name(), self.ex_manager.default_xs._exchange, str(sentinel.xp))

        self.assertEquals(xp._exchange, sentinel.xp)
        self.assertEquals(xp._xs, self.ex_manager.default_xs)
        self.assertEquals(xp._xptype, 'ttree')
        self.assertEquals(xp._queue, None)
        self.assertEquals(xp._binding, None)

        self.assertEquals(xp.exchange, exstr)

        # declaration
        self.ex_manager._transport.declare_exchange_impl.assert_called_once_with(self.ex_manager._client, exstr, auto_delete=True, durable=False, exchange_type='topic')

    def test_create_xp_with_params(self):
        xp = self.ex_manager.create_xp(sentinel.xp, xptype=sentinel.xptype)
        self.assertEquals(xp._xptype, sentinel.xptype)

    def test_create_xp_with_different_xs(self):
        xs = self.ex_manager.create_xs(sentinel.xs)
        xs_exstr = '%s.ion.xs.%s' % (get_sys_name(), str(sentinel.xs))     # what we expect the exchange property to return

        xp = self.ex_manager.create_xp(sentinel.xp, xs)
        xp_exstr = '%s.xp.%s' % (xs_exstr, str(sentinel.xp))

        # check mappings
        self.assertIn(sentinel.xp, self.ex_manager.xn_by_name)
        self.assertIn(xp, self.ex_manager.xn_by_xs[sentinel.xs])

        self.assertEquals(xp.exchange, xp_exstr)

    def test_delete_xp(self):
        xp      = self.ex_manager.create_xp(sentinel.xp)
        exstr   = "%s.ion.xs.%s.xp.%s" % (get_sys_name(), self.ex_manager.default_xs._exchange, str(sentinel.xp))

        self.assertIn(sentinel.xp, self.ex_manager.xn_by_name)

        self.ex_manager.delete_xp(xp)

        self.assertNotIn(sentinel.xp, self.ex_manager.xn_by_name)

        # deletion
        self.ex_manager._transport.delete_exchange_impl.assert_called_once_with(self.ex_manager._client, exstr)

    def test_delete_xp_without_creating_it_first(self):
        xpmock = Mock(ExchangePoint)
        xpmock._exchange = sentinel.delete_me

        self.assertRaises(KeyError, self.ex_manager.delete_xp, xpmock)

    def test__create_xn_unknown_type(self):
        self.assertRaises(StandardError, self.ex_manager._create_xn, sentinel.unknown)

    def test_create_xn_service(self):
        xn      = self.ex_manager.create_xn_service('servicename')
        qstr    = '%s.%s' % (xn.exchange, 'servicename')        # what we expect the queue name to look like

        self.assertIsInstance(xn, ExchangeName)
        self.assertIsInstance(xn, ExchangeNameService)

        # exclusive attrs to XN
        self.assertEquals(xn._xs, self.ex_manager.default_xs)
        self.assertEquals(xn._xn_auto_delete, ExchangeNameService._xn_auto_delete)
        self.assertEquals(xn._xn_durable, ExchangeNameService._xn_durable)
        self.assertEquals(xn.xn_type, 'XN_SERVICE')

        # underlying attrs
        self.assertEquals(xn._exchange, None)
        self.assertEquals(xn._queue, 'servicename')
        self.assertEquals(xn._binding, None)

        # top level props
        self.assertEquals(xn.exchange, self.ex_manager.default_xs.exchange)
        self.assertEquals(xn.queue, qstr)
        self.assertEquals(xn.binding, 'servicename')

        # should be in mapping
        self.assertIn('servicename', self.ex_manager.xn_by_name)
        self.assertIn(xn, self.ex_manager.xn_by_xs[ION_ROOT_XS])

        # declaration
        self.ex_manager._transport.declare_queue_impl.assert_called_once(self.ex_manager._client, qstr, durable=ExchangeNameService._xn_durable, auto_delete=ExchangeNameService._xn_auto_delete)

    def test_create_xn_process(self):
        xn = self.ex_manager.create_xn_process('procname')

        self.assertIsInstance(xn, ExchangeName)
        self.assertIsInstance(xn, ExchangeNameProcess)

    def test_create_xn_queue(self):
        xn = self.ex_manager.create_xn_queue('queuename')

        self.assertIsInstance(xn, ExchangeName)
        self.assertIsInstance(xn, ExchangeNameQueue)

    def test_create_xn_with_different_xs(self):
        xs = self.ex_manager.create_xs(sentinel.xs)
        xs_exstr = '%s.ion.xs.%s' % (get_sys_name(), str(sentinel.xs))     # what we expect the exchange property to return

        xn      = self.ex_manager.create_xn_service('servicename', xs)
        qstr    = '%s.%s' % (xn.exchange, 'servicename')        # what we expect the queue name to look like

        # check mappings
        self.assertIn('servicename', self.ex_manager.xn_by_name)
        self.assertIn(xn, self.ex_manager.xn_by_xs[sentinel.xs])

        self.assertEquals(xn.queue, qstr)

    def test_delete_xn(self):
        xn      = self.ex_manager.create_xn_process('procname')
        qstr    = '%s.%s' % (xn.exchange, 'procname')

        self.assertIn('procname', self.ex_manager.xn_by_name)

        self.ex_manager.delete_xn(xn)

        self.assertNotIn('procname', self.ex_manager.xn_by_name)

        # call to broker
        self.ex_manager._transport.delete_queue_impl.assert_called_once_with(self.ex_manager._client, qstr)

    def test_xn_setup_listener(self):
        xn      = self.ex_manager.create_xn_service('servicename')
        qstr    = '%s.%s' % (xn.exchange, 'servicename')        # what we expect the queue name to look like

        xn.setup_listener(sentinel.binding, None)

        self.ex_manager._transport.bind_impl.assert_called_once_with(self.ex_manager._client, xn.exchange, qstr, sentinel.binding)

    def test_xn_bind(self):
        xn      = self.ex_manager.create_xn_service('servicename')

        xn.bind(sentinel.bind)

        self.ex_manager._transport.bind_impl.assert_called_once_with(self.ex_manager._client, xn.exchange, xn.queue, sentinel.bind)

    def test_xn_unbind(self):
        xn      = self.ex_manager.create_xn_service('servicename')

        xn.unbind(sentinel.bind)

        self.ex_manager._transport.unbind_impl.assert_called_once_with(self.ex_manager._client, xn.exchange, xn.queue, sentinel.bind)


@attr('INT', group='exchange')
class TestExchangeObjectsInt(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_rpc_with_xn(self):
        # get an xn to use for send/recv
        xn = self.container.ex_manager.create_xn_service('hello')

        # create an RPCServer for a hello service
        hs = HelloService()
        rpcs = RPCServer(from_name=xn, service=hs)

        # spawn the listener, kill on test exit (success/fail/error should cover?)
        gl_listen = spawn(rpcs.listen)
        self.addCleanup(gl_listen.kill)

        # ok, now create a client using same xn
        hsc = HelloServiceClient(to_name=xn)

        # try to message it!
        ret = hsc.hello('hi there')

        # did we get back what we expected?
        self.assertEquals(ret, 'BACK:hi there')

    def test_pubsub_with_xp(self):
        raise unittest.SkipTest("not done yet")

    def test_exchange_by_name(self):
        raise unittest.SkipTest("not done yet")

    def test_create_xs(self):
        raise unittest.SkipTest("not done yet")

    def test_create_xp(self):
        raise unittest.SkipTest("not done yet")

    def test_create_xn(self):
        raise unittest.SkipTest("not done yet")

    def test_delete_xs(self):
        raise unittest.SkipTest("not done yet")

    def test_delete_xp(self):
        raise unittest.SkipTest("not done yet")

    def test_delete_xn(self):
        raise unittest.SkipTest("not done yet")