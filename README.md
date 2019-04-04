# Motiv
**Simple and Efficient processing pipelining.**

[![Build Status](https://travis-ci.com/SaadTalaat/motiv.svg?branch=master)](https://travis-ci.com/SaadTalaat/motiv)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/SaadTalaat/motiv.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/SaadTalaat/motiv/alerts/)
[![codecov](https://codecov.io/gh/SaadTalaat/motiv/branch/master/graph/badge.svg)](https://codecov.io/gh/SaadTalaat/motiv)
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://github.com/SaadTalaat/motiv)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/pymotiv)
[![PyPI](https://img.shields.io/pypi/v/motiv.svg)](https://pypi.org/project/motiv/)

Motiv is a framework to ease and enables building minimal pipelines and pipeline-like applications. It abstracts away communication, messaging patterns and execution patterns.

## When to use Motiv?
Motiv is for minimal pipelining and pipeline-like applications, you can build a tightly-coupled or a loosely-coupled pipeline using it. However, if you're pipeline's purpose is data processing, motiv is a good start. If the purpose is *big* data processing, then you're better off using a big data processing engine (e.g. Spark).

## Why Motiv?
Motiv is simple, it provides many messaging-patterns, it manages them for you and it provides you direct control over them. Motiv is stateless, and so should the applications that use it. Motiv uses efficient and very fast communication/message-passing ([zmq](http://zeromq.org/)) for the underlying communication.

## Installation

```bash
$ pip install motiv
```
or (recommended)
```bash
$ git clone git@github.com:SaadTalaat/motiv.git
$ cd motiv
$ python3 setup.py install
```

## Quickstart
Motiv has two type of patterns that are glued together to form a component in a pipeline. A stream (messaging-pattern) and an actor (execution-pattern). Streams can be a `Emitter`, `Subscriber`, `Ventilator`, `Worker`, `Sink`. an actor can be `Ticker` or `Proxy`.

**Streams?**

Streams are boilerplate messaging-patterns that either send out messages or receive messages. for example an `Emitter` has a typical publisher behavior. Unlike the `Emitter` A `Ventilator` distributes messages between listening components (workers).

**Execution patterns?**

An execution pattern defines how a component should execute, for example, a component can simply be a `Proxy` between two streams that forwards messages received over subscriber stream to a ventilator, or from a worker to a publisher, or from a worker to a sink. It can also be a `Ticker` which is execute-till-halt event-loop with each cycle calling `Ticker.tick` method function, this type of behavior is convenient for processing incoming messages and sending out results to different actors or do an action depending on these messages.

**Streams + Execution Pattern**

An execution pattern or an actor expects to have an input stream or output stream or both for it to be runnable.

## Examples
See examples: [motiv/examples/](https://github.com/SaadTalaat/motiv/tree/master/motiv/examples)

Run examples,
```bash
$ git clone git@github.com:SaadTalaat/motiv.git
$ cd motiv
$ python3 motiv/examples/workers.py
....
$ python3 motiv/examples/pubsub.py
```
## Disclaimer
Motiv is still in development, master might not be stable, features will be rolled out fast. I encourage you to fix any code you see wrong, raise any issues that concern you and actively participate in developing *Motiv*.
