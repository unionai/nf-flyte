# nf-flyte

## Summary

`nf-flyte` runs Nextflow pipelines on [Flyte v2](https://github.com/flyteorg/flyte).
It adds a `flyte` executor that runs each Nextflow task as a Flyte action. The Nextflow
head itself runs as a Flyte task, so the whole pipeline shows up as one Flyte run with
one child action per task.

- Each task runs in its own `container` with no extra tooling: Flyte moves the task's data.
  Its input files are Flyte `File`/`Dir` inputs, and its work dir comes back as a `Dir`
  output, staged by Flyte's copilot. Task pods never touch the work dir themselves.
- Nextflow keeps retries (`errorStrategy`) and `-resume`. Flyte retries are off for task actions.
- Flyte caches task results under a key derived from the task's script, container, inputs and
  `bin` scripts but not the Nextflow session, so a task done in one run is reused by the next.
- A failed task shows as failed in Flyte and is never cached; Nextflow still gets its exit
  code, stdout and stderr.
- Killing a task or cancelling the pipeline aborts the matching Flyte actions.

## Get Started

Run Nextflow from inside a Flyte v2 task. The Flyte runtime provides the endpoint,
org/project/domain, run location and API key through the task's environment. The task
also needs to set `NF_FLYTE_RUN_NAME` (and `NF_FLYTE_PARENT_ACTION` when it isn't `a0`).

```groovy
plugins {
    id 'nf-flyte@0.1.0'
}

process.executor = 'flyte'
workDir = 's3://<bucket>/<path>'
```

**Work directory:** any object store that both the Nextflow head and Flyte can read, e.g. the
task's own Flyte storage (the default with `flyteplugins-nextflow`). S3, GCS and Azure all
work, through Nextflow's `nf-amazon`, `nf-google` and `nf-azure` plugins.

**Task containers** need `bash`, nothing else.

### Launching outside Flyte

You can also run `nextflow run` on a laptop or in CI. The executor then creates the Flyte run
itself and prints its URL. Its root action stands for the Nextflow head:
- It ends with the pipeline's result. Stopping Nextflow (Ctrl+C, SIGTERM) aborts the run,
  which then shows as aborted rather than failed.
- It fails if the head stops sending heartbeats (for example, a closed laptop or a killed CI
  job), which also aborts the run's remaining tasks.

```groovy
flyte {
    endpoint = 'tenant.hosted.unionai.cloud'  // default: from the API key
    org = 'my-org'                            // default: from the API key
    project = 'my-project'
    domain = 'development'
    // either `export FLYTE_API_KEY=...`, or a command that prints an access token:
    // authCommand = 'my-cli print-token'
    heartbeatTimeout = '5m'                   // default: 5m
}
workDir = 's3://<bucket>/<path>'              // readable by Flyte, writable from where you run
```

## Examples

```groovy
flyte {
    queue = 'gpu'          // Flyte queue for task actions (default: the run's queue)
    pollInterval = '10s'   // how often to poll task status (default: 5s)
    cache = true           // cache task results in Flyte (default: true)
}
```

All `flyte` options are optional inside Flyte. Each one defaults to a value from the
task's environment:

| Option          | Default source                     |
|-----------------|------------------------------------|
| `endpoint`      | `_U_EP_OVERRIDE`                   |
| `org`           | `_U_ORG_NAME`                      |
| `project`       | `FLYTE_INTERNAL_EXECUTION_PROJECT` |
| `domain`        | `FLYTE_INTERNAL_EXECUTION_DOMAIN`  |
| `runName`       | `NF_FLYTE_RUN_NAME`                |
| `parentAction`  | `NF_FLYTE_PARENT_ACTION`, else `a0` |
| `runOutputBase` | `_U_RUN_BASE`                      |

The API key comes from `_UNION_EAGER_API_KEY`, `EAGER_API_KEY` or `FLYTE_API_KEY`.
It is never read from the config file.

Process directives map to the Flyte action like this:

| Nextflow                  | Flyte                                              |
|---------------------------|----------------------------------------------------|
| `container`               | container image (required)                         |
| `cpus`, `memory`, `disk`  | CPU / MEMORY / EPHEMERAL_STORAGE requests = limits |
| `accelerator`             | GPU                                                |
| `time`                    | task timeout                                       |

### How a task runs

1. The head renders Nextflow's usual task wrapper (`.command.run`), with the pod's paths, and
   writes it and the task's `.flyte/inputs.pb` into the task dir.
2. Flyte's copilot downloads every input file into the pod, keeping its name. The wrapper runs
   the task in a scratch dir, linking the inputs under their Nextflow stage names, and copies the
   declared outputs and the `.command.*` files to the directory copilot uploads.
3. On success, the head copies that directory into the task dir, where Nextflow collects the
   outputs. On failure, the pod reports the exit code and the end of stdout/stderr through
   copilot's error file: Flyte fails the action (without caching it), and the head writes
   `.exitcode`, `.command.out` and `.command.err` back for Nextflow's `errorStrategy`.

If the pod itself dies (e.g. it's OOM-killed), the task gets the pod's exit code (e.g. `137`),
so exit-code-based retry rules keep working.

### Caching

The Flyte cache key covers what Nextflow's task hash covers except the session: the process,
its rendered script, container, environment and `bin` scripts, and its input files. An input
produced by another task is identified by that task's key (saved in its task dir as
`.flyte/cache_key`), so keys chain through the pipeline without reading file contents. Processes
with `cache false`, and all tasks with `flyte.cache = false`, are never cached.

## Plugin development

```bash
make assemble   # build
make test       # unit tests
make install    # install into ~/.nextflow/plugins
make release    # publish to the Nextflow registry (needs npr.apiKey in ~/.gradle/gradle.properties)
```

## License

[Apache License 2.0](COPYING)
