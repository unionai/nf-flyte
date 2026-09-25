package unionai.plugin.executor

import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.CompileStatic
import nextflow.executor.BashWrapperBuilder
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

/**
 * Nextflow's own task wrapper (`.command.run`), rendered for a Flyte task pod.
 *
 * Flyte's copilot moves the data: it downloads each staged input to `/var/inputs/<var>` and,
 * when the task ends, uploads `/var/outputs/task_dir` as the action's `task_dir` output. So
 * the wrapper is rendered as if the task's work dir were `/var/outputs/task_dir` and its
 * inputs lived under `/var/inputs`, and it runs in a scratch dir: staged-input links stay in
 * scratch, and only declared outputs plus the `.command.*` files reach the uploaded dir.
 * The rendered files are still written to the task's real work dir, where the head keeps them.
 */
@CompileStatic
class FlyteTaskLauncher extends BashWrapperBuilder {

    static final String POD_INPUTS = '/var/inputs'
    static final String POD_OUTPUTS = '/var/outputs'
    static final String TASK_DIR_OUTPUT = 'task_dir'
    static final String POD_TASK_DIR = "$POD_OUTPUTS/$TASK_DIR_OUTPUT"
    static final String BIN_INPUT = 'nf_bin'
    static final String ERROR_FILE = '_ERROR'
    static final String FAILURE_PREFIX = 'nf-flyte task failed: exit='

    /**
     * The task pod's entry point: put the `.command.*` files where the wrapper expects them,
     * run it, and report a failed task through copilot's `_ERROR` file. Copilot then skips
     * the output upload and Flyte fails the action (so it's visible, and never cached) even
     * though the container exits 0. The exit code and output tails go into the error message,
     * which the head turns back into `.exitcode`, `.command.err` and `.command.out`.
     */
    static final String ENTRYPOINT = """\
        set -u
        shopt -s dotglob nullglob
        W=$POD_TASK_DIR
        mkdir -p "\$W"
        for f in $POD_INPUTS/nf_cmd_*/*; do cp "\$f" "\$W/"; done
        if [ -d $POD_INPUTS/$BIN_INPUT ]; then
            chmod -R a+x $POD_INPUTS/$BIN_INPUT
            export PATH="$POD_INPUTS/$BIN_INPUT:\$PATH"
        fi
        # forward SIGTERM (abort, pod shutdown) to the wrapper, which stops the task
        bash "\$W/${TaskRun.CMD_RUN}" & pid=\$!
        trap 'kill -TERM \$pid 2>/dev/null' TERM
        wait \$pid; rc=\$?
        if kill -0 \$pid 2>/dev/null; then wait \$pid; rc=\$?; fi
        if [ "\$rc" -ne 0 ]; then
            {
                echo "${FAILURE_PREFIX}\$rc"
                echo '--- stderr (tail)'
                tail -c 16000 "\$W/${TaskRun.CMD_ERRFILE}" 2>/dev/null
                echo '--- stdout (tail)'
                tail -c 4000 "\$W/${TaskRun.CMD_OUTFILE}" 2>/dev/null
            } > $POD_OUTPUTS/$ERROR_FILE
        fi
        exit 0
        """.stripIndent()

    /** A Nextflow input file or directory, delivered to the pod as a copilot File / Dir input */
    @CompileStatic
    static class StagedInput {
        final String var
        final Path source
        final String stageName
        final boolean dir

        StagedInput(String var, Path source, String stageName, boolean dir) {
            this.var = var
            this.source = source
            this.stageName = stageName
            this.dir = dir
        }

        /** Where copilot puts it (NAMED_DIR layout): a Dir's contents, or a File under its own name */
        String getPodPath() {
            return dir ? "$POD_INPUTS/$var" : "$POD_INPUTS/$var/${source.fileName}"
        }
    }

    private final Path realWorkDir

    private FlyteTaskLauncher(TaskBean podBean, Path realWorkDir) {
        super(podBean, new SimpleFileCopyStrategy(podBean))
        this.realWorkDir = realWorkDir
    }

    /**
     * @param task the Nextflow task
     * @param inputs where each of the task's staged inputs will be in the pod
     */
    static FlyteTaskLauncher create(TaskRun task, List<StagedInput> inputs) {
        final bean = task.toTaskBean()
        final realWorkDir = bean.workDir
        final podWorkDir = Paths.get(POD_TASK_DIR)
        bean.workDir = podWorkDir
        bean.targetDir = podWorkDir
        if( bean.scratch == null || bean.scratch == false )
            bean.scratch = true
        bean.shell = containerShell(bean.shell)
        final podInputs = new LinkedHashMap<String,Path>()
        for( StagedInput it : inputs )
            podInputs.put(it.stageName, Paths.get(it.podPath))
        bean.inputFiles = podInputs
        return new FlyteTaskLauncher(bean, realWorkDir)
    }

    /**
     * The task script's interpreter comes from the head's process `shell`. Nix-packaged Nextflow
     * (e.g. installed with devbox) defaults it to the head's `/nix/store/.../bin/bash`, which
     * never exists in a task container: use the container's `/bin/<name>` instead.
     */
    static List<String> containerShell(List<String> shell) {
        if( !shell )
            return shell
        final m = shell[0] =~ /^\/nix\/store\/[^\/]+\/bin\/([^\/]+)$/
        if( !m.matches() )
            return shell
        final result = new ArrayList<String>(shell)
        result[0] = "/bin/${m.group(1)}".toString()
        return result
    }

    // render with pod paths, but write the files to the task's real work dir

    @Override
    protected Path targetWrapperFile() { realWorkDir.resolve(TaskRun.CMD_RUN) }

    @Override
    protected Path targetScriptFile() { realWorkDir.resolve(TaskRun.CMD_SCRIPT) }

    @Override
    protected Path targetInputFile() { realWorkDir.resolve(TaskRun.CMD_INFILE) }

    @Override
    protected Path targetStageFile() { realWorkDir.resolve(TaskRun.CMD_STAGE) }
}
