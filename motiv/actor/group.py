"""Actor groups, and scalability"""

from motiv import exceptions as excs
from motiv.actor.process import ExecutionContext, ProcessEvent


class ActorGroup:
    """Wrapper for homogeneous n number of actors

    This type provides a uniform way to control a set of
    homogeneous actors
    """
    def __init__(self, name, count, actor_type, *args, **kwargs):
        if not issubclass(actor_type, ExecutionContext):
            raise TypeError(f"actor_type must be ExecutionContext,"
                            f" not {actor_type}")
        if count <= 0:
            raise ValueError("count must be a positive integer")

        self.actor_type = actor_type
        self.count = count
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.runnables = []
        self.started = ProcessEvent()
        self.stopped = ProcessEvent()

    def start(self):
        """Starts n number of actors"""
        if self.started.is_set():
            raise excs.DispatchError("group already started")

        for i in range(self.count):
            member_name = f"{self.name}[{i}]"
            actor = self.actor_type(member_name, *self.args, **self.kwargs)
            actor.start()
            self.runnables.append(actor)
        self.started.set()

    def stop(self):
        """Stops all started actors"""
        if not self.started.is_set():
            raise excs.DispatchError("group didn't started")

        if self.stopped.is_set():
            raise excs.DispatchError("group already stopped")

        for runnable in self.runnables:
            runnable.stop()
        self.stopped.set()

    def join(self):
        """Waits for all child actors to terminate"""
        for runnable in self.runnables:
            runnable.join()


class ScalableActorGroup(ActorGroup):
    """Wrapper for homogeneous dynamic actor group

    Unline `ActorGroup`, this type allows adding and removing
    a subset of child actors"""

    def scale_up(self, count):
        """adds n number of actors to current child actors"""
        if count <= 0:
            raise excs.ScalingError("count must be a positive integer")

        if self.stopped.is_set():
            raise excs.ScalingError("cannot scale a group"
                                    "after it being stopped.")

        if not self.started.is_set():
            self.count += count

        else:
            for _ in range(count):
                member_name = f"{self.name}[{self.count}]"
                self.count += 1
                actor = self.actor_type(member_name, *self.args, **self.kwargs)
                actor.start()
                self.runnables.append(actor)

    def scale_down(self, count):
        """removes/stops n number of actors from current child actors"""
        if count <= 0:
            raise excs.ScalingError("count must be positive integer")

        if count > self.count:
            raise excs.ScalingError(f"cannot scale down {count} runnables,"
                                    f" current size is {self.count}")

        if self.stopped.is_set():
            raise excs.ScalingError("cannot scale down a group"
                                    "after it being stopped.")

        if not self.started.is_set():
            self.count -= count
        else:
            stopped = []
            for _ in range(count):
                actor = self.runnables.pop()
                actor.stop()
                self.count -= 1
                stopped.append(actor)

            for actor in stopped:
                actor.join()


__all__ = [
        'ActorGroup',
        'ScalableActorGroup'
        ]
