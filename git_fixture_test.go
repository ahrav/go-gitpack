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

// gitNeutralEnviron returns os.Environ() with every repo-selection variable
// dropped, preserving all unrelated entries.
func gitNeutralEnviron() []string {
	parent := os.Environ()
	out := make([]string, 0, len(parent))
	for _, kv := range parent {
		name, _, ok := strings.Cut(kv, "=")
		if ok && slices.Contains(gitRepoSelectionVars, name) {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// gitFixtureEnv must not carry a repo-selection variable through from the
// parent environment, or every fixture would build its commits in whatever
// repository that variable names. Blanking is not a substitute: GIT_DIR="" is
// an explicit invalid path to Git, not an unset variable.
func TestGitFixtureEnvDropsInheritedRepoSelection(t *testing.T) {
	for _, name := range gitRepoSelectionVars {
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
		if slices.Contains(gitRepoSelectionVars, name) {
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
