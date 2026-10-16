# Pipeline Debugger for Redpanda Connect

Step a captured batch of messages through a [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) pipeline, processor by processor, in the VS Code debugger.

> **Status: tracer bullet.** Set `rpcnDebug.binaryPath` to an `rpcn-debug` binary, open a pipeline YAML file and press F5: the session captures one batch, stops before the first pipeline processor with the messages under Variables, and Step Over or Continue runs it to the end.

Source, build instructions and license: <https://github.com/ajurcenk/rpcn-debugger>.
