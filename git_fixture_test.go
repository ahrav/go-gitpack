// git_fixture_test.go is the single source of git-environment hygiene for
// test and benchmark fixtures. Every fixture runner routes through
// gitFixtureEnv so isolation fixes (config isolation, auto-maintenance and
// signing disablement) propagate to all fixtures instead of drifting across
// per-file copies.
package objstore

import (
	"os"
	"strings"
	"testing"
)

// gitFixtureEnv isolates fixture creation from the invoking user's Git
// configuration and pins the commit identity, mirroring the hygiene in
// generate_testdata.sh. Background auto-gc and maintenance are disabled so
// nothing repacks a fixture behind a test's back, and commit signing is
// disabled so fixtures build on hosts with global commit.gpgsign=true.
func gitFixtureEnv() []string {
	return append(os.Environ(),
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
