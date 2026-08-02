// git_fixture_test.go is the single source of git-environment hygiene for
// test and benchmark fixtures. Every fixture runner routes through
// gitFixtureEnv so isolation fixes (config isolation, auto-maintenance and
// signing disablement) propagate to all fixtures instead of drifting across
// per-file copies.
package objstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// gitFixtureEnv isolates fixture creation from the invoking user's Git
// configuration and pins the commit identity, mirroring the hygiene in
// generate_testdata.sh. Background auto-gc and maintenance are disabled so
// nothing repacks a fixture behind a test's back, and commit signing is
// disabled so fixtures build on hosts with global commit.gpgsign=true.
//
// Repo-selection variables are removed from the inherited environment rather
// than blanked: for path-valued Git variables an empty string is an explicit
// (invalid) path, not "unset", so only removal restores Git's own discovery.
// Without this, running the suite from a context that exports GIT_DIR -- a hook,
// `git bisect run`, `git rebase --exec` -- would aim git init/add/commit at that
// repository instead of the fixture's temporary directory.
func gitFixtureEnv() []string {
	return append(gitNeutralEnviron(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_TEMPLATE_DIR=",
		"GIT_AUTHOR_NAME=bench",
		"GIT_AUTHOR_EMAIL=bench@example.com",
		"GIT_COMMITTER_NAME=bench",
		"GIT_COMMITTER_EMAIL=bench@example.com",
		"GIT_CONFIG_COUNT=3",
		"GIT_CONFIG_KEY_0=gc.auto", "GIT_CONFIG_VALUE_0=0",
		"GIT_CONFIG_KEY_1=maintenance.auto", "GIT_CONFIG_VALUE_1=false",
		"GIT_CONFIG_KEY_2=commit.gpgsign", "GIT_CONFIG_VALUE_2=false",
	)
}

// gitFixtureEnvPinned is gitFixtureEnv with both commit dates pinned, for
// fixtures whose object IDs or timestamp collisions must be reproducible.
// date uses any format git accepts, e.g. "2005-04-07T22:13:13 +0000".
func gitFixtureEnvPinned(date string) []string {
	return append(gitFixtureEnv(),
		"GIT_AUTHOR_DATE="+date,
		"GIT_COMMITTER_DATE="+date,
	)
}

// requireGit skips the caller when no git executable is available, which is
// every fixture's precondition.
func requireGit(tb testing.TB) {
	tb.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		tb.Skip("git executable not found in PATH")
	}
}

// runGit runs one git command against repo under the hermetic fixture
// environment, failing the test on error.
func runGit(tb testing.TB, repo string, args ...string) {
	tb.Helper()
	runGitEnv(tb, repo, gitFixtureEnv(), args...)
}

// runGitEnv is runGit with a caller-supplied environment (typically
// gitFixtureEnv or gitFixtureEnvPinned, possibly extended).
func runGitEnv(tb testing.TB, repo string, env []string, args ...string) {
	tb.Helper()
	cmd := gitTestCommand(repo, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

// gitRepoSelectionVars are the environment variables that redirect which
// repository, work tree, index, or object store a git invocation acts on.
var gitRepoSelectionVars = []string{
	"GIT_DIR",
	"GIT_WORK_TREE",
	"GIT_INDEX_FILE",
	"GIT_OBJECT_DIRECTORY",
	"GIT_ALTERNATE_OBJECT_DIRECTORIES",
	"GIT_COMMON_DIR",
	"GIT_NAMESPACE",
	"GIT_CEILING_DIRECTORIES",
	"GIT_DISCOVERY_ACROSS_FILESYSTEM",
}

// gitConfigInjectionVars are the environment variables that inject Git
// configuration into an invocation, as opposed to naming a config FILE.
// GIT_CONFIG_GLOBAL and GIT_CONFIG_SYSTEM are not here: gitFixtureEnv points
// both at /dev/null, so an inherited value is superseded rather than dropped.
//
// GIT_CONFIG_PARAMETERS is the load-bearing one. Git exports it to carry a
// parent invocation's `-c` settings down to hooks and subprocesses, and it
// takes precedence over the GIT_CONFIG_COUNT/KEY/VALUE triples gitFixtureEnv
// appends -- verified on git 2.50.1, where an inherited
// GIT_CONFIG_PARAMETERS="'commit.gpgsign'='true'" wins over an explicit
// GIT_CONFIG_KEY_n=commit.gpgsign / GIT_CONFIG_VALUE_n=false. Running the suite
// under `git -c ...` (a hook, `git bisect run`, `git rebase --exec`) would
// therefore re-enable the very signing and auto-maintenance the fixture
// environment disables.
var gitConfigInjectionVars = []string{
	"GIT_CONFIG_PARAMETERS",
	"GIT_CONFIG_COUNT",
}

// gitConfigInjectionPrefixes match the numbered config triples, whose indices
// are unbounded and so cannot be enumerated. Dropping them means gitFixtureEnv
// supplies the complete triple set rather than relying on later duplicate
// entries in the environment winning.
var gitConfigInjectionPrefixes = []string{
	"GIT_CONFIG_KEY_",
	"GIT_CONFIG_VALUE_",
}

// gitEnvMustDrop reports whether an environment variable named name would let
// the parent process redirect or reconfigure a fixture's git invocation.
func gitEnvMustDrop(name string) bool {
	if slices.Contains(gitRepoSelectionVars, name) ||
		slices.Contains(gitConfigInjectionVars, name) {
		return true
	}
	for _, prefix := range gitConfigInjectionPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// gitNeutralEnviron returns os.Environ() with every repo-selection and
// config-injection variable dropped, preserving all unrelated entries.
func gitNeutralEnviron() []string {
	parent := os.Environ()
	out := make([]string, 0, len(parent))
	for _, kv := range parent {
		name, _, ok := strings.Cut(kv, "=")
		if ok && gitEnvMustDrop(name) {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// gitFixtureEnv must not carry a repo-selection or config-injection variable
// through from the parent environment. A repo-selection variable would make
// every fixture build its commits in whatever repository that variable names;
// a config-injection variable would reconfigure the invocation, and
// GIT_CONFIG_PARAMETERS specifically outranks the triples gitFixtureEnv sets.
// Blanking is not a substitute for either: GIT_DIR="" is an explicit invalid
// path to Git, not an unset variable.
func TestGitFixtureEnvDropsInheritedRepoSelection(t *testing.T) {
	hostile := append([]string{}, gitRepoSelectionVars...)
	hostile = append(hostile, gitConfigInjectionVars...)
	for _, prefix := range gitConfigInjectionPrefixes {
		hostile = append(hostile, prefix+"0", prefix+"7")
	}
	for _, name := range hostile {
		t.Setenv(name, "/somewhere/else")
	}
	const sentinel = "GO_GITPACK_FIXTURE_ENV_SENTINEL"
	t.Setenv(sentinel, "preserved")

	var leaked []string
	sawSentinel := false
	for _, kv := range gitFixtureEnv() {
		name, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		// Checked against the hostile list this test built, NOT against
		// gitEnvMustDrop: reusing the production predicate as the oracle would
		// make the assertion vacuous, since narrowing the predicate would
		// narrow the check with it.
		//
		// gitFixtureEnv appends its own config triples, so a name it also sets
		// is a leak only while it still carries the parent's value.
		if slices.Contains(hostile, name) && strings.HasSuffix(kv, "=/somewhere/else") {
			leaked = append(leaked, kv)
		}
		if name == sentinel {
			sawSentinel = true
		}
	}

	require.Empty(t, leaked, "repo-selection variables must not reach a fixture git invocation")
	require.True(t, sawSentinel, "filtering must preserve unrelated environment entries")
}

// A host whose global config demands commit signing through a program that
// does not exist must not break fixture creation: gitFixtureEnv disables
// signing and neutralizes the global config, so the commit succeeds.
func TestRunGitIgnoresHostileGlobalCommitSigning(t *testing.T) {
	home := t.TempDir()
	missingGPG := filepath.Join(home, "missing-gpg")
	globalConfig := []byte("[commit]\n\tgpgsign = true\n[gpg]\n\tprogram = " + missingGPG + "\n")
	require.NoError(t, os.WriteFile(filepath.Join(home, ".gitconfig"), globalConfig, 0o644))
	hostileEnv := append(gitFixtureEnv(), "HOME="+home, "XDG_CONFIG_HOME="+home)

	repo := t.TempDir()
	runGitEnv(t, repo, hostileEnv, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "file.txt"), []byte("content\n"), 0o644))
	runGitEnv(t, repo, hostileEnv, "add", "file.txt")
	runGitEnv(t, repo, hostileEnv, "commit", "-m", "must not sign", "--quiet")
}

// A parent `git -c commit.gpgsign=true ...` exports GIT_CONFIG_PARAMETERS to
// its subprocesses, and Git ranks that above the GIT_CONFIG_COUNT/KEY/VALUE
// triples gitFixtureEnv sets -- so inheriting it would re-enable signing with a
// nonexistent gpg program and break every fixture. Dropping it is what keeps
// the suite runnable from inside a hook, `git bisect run`, or
// `git rebase --exec`.
func TestRunGitIgnoresInheritedConfigParameters(t *testing.T) {
	requireGit(t)

	home := t.TempDir()
	missingGPG := filepath.Join(home, "missing-gpg")
	t.Setenv("GIT_CONFIG_PARAMETERS",
		"'commit.gpgsign'='true' 'gpg.program'='"+missingGPG+"'")

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "file.txt"), []byte("content\n"), 0o644))
	runGit(t, repo, "add", "file.txt")
	runGit(t, repo, "commit", "-m", "must not sign", "--quiet")
}
