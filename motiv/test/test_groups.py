import unittest
import time

from motiv import exceptions as excs
from motiv.actor import Ticker, ActorGroup, ScalableActorGroup


class TickerMock(Ticker):

    def pre_start(self):
        pass

    def post_stop(self):
        pass

    def tick(self):
        time.sleep(0.001)


class TestActorGroup(unittest.TestCase):

    def test_wrong_actor_type(self):
        with self.assertRaises(TypeError):
            ActorGroup("test_group", 1, int)

    def test_negative_actor_count(self):
        with self.assertRaises(ValueError):
            ActorGroup("test_group", -1, TickerMock)

    def test_starting_twice(self):
        g = ActorGroup("test_group", 5, TickerMock)
        g.start()
        with self.assertRaises(excs.DispatchError):
            g.start()
        g.stop()
        g.join()

    def test_stopping_twice(self):
        g = ActorGroup("test_group", 5, TickerMock)
        g.start()
        g.stop()
        with self.assertRaises(excs.DispatchError):
            g.stop()
        g.join()

    def test_starting_stopped_group(self):
        g = ActorGroup("test_group", 5, TickerMock)
        g.start()
        g.stop()
        with self.assertRaises(excs.DispatchError):
            g.start()
        g.join()

    def test_stopping_before_starting(self):
        g = ActorGroup("test_group", 5, TickerMock)
        with self.assertRaises(excs.DispatchError):
            g.stop()


class TestScalableActorGroup(unittest.TestCase):

    def test_scaling_up_negative(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        with self.assertRaises(excs.ScalingError):
            g.scale_up(-1)

    def test_scaling_down_negative(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        with self.assertRaises(excs.ScalingError):
            g.scale_down(-1)

    def test_scaling_up_stopped_group(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        g.start()
        g.stop()
        with self.assertRaises(excs.ScalingError):
            g.scale_up(1)
        g.join()

    def test_scaling_down_stopped_group(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        g.start()
        g.stop()
        with self.assertRaises(excs.ScalingError):
            g.scale_down(1)
        g.join()

    def test_over_scaling_down(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        g.start()
        with self.assertRaises(excs.ScalingError):
            g.scale_down(10)
        g.stop()
        g.join()

    def test_scaling_up_after_starting(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        self.assertEqual(g.count, 5)
        g.start()
        g.scale_up(3)
        self.assertEqual(len(g.runnables), g.count, 8)
        g.stop()
        g.join()

    def test_scaling_down_after_starting(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        self.assertEqual(g.count, 5)
        g.start()
        g.scale_down(3)
        self.assertEqual(len(g.runnables), g.count, 2)
        g.stop()
        g.join()

    def test_scaling_up_not_started_group(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        self.assertEqual(g.count, 5)
        g.scale_up(3)
        self.assertEqual(g.count, 8)
        g.start()
        self.assertEqual(len(g.runnables), g.count, 8)
        g.stop()
        g.join()

    def test_scaling_down_not_started_group(self):
        g = ScalableActorGroup("test_group", 5, TickerMock)
        self.assertEqual(g.count, 5)
        g.scale_down(3)
        self.assertEqual(g.count, 2)
        g.start()
        self.assertEqual(len(g.runnables), g.count, 2)
        g.stop()
        g.join()
