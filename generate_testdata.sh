#!/bin/bash
# generate_testdata.sh - Generate test Git repositories with guaranteed pack files

set -e

TESTDATA_DIR="testdata/repos"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Generating test data for Git pack tests...${NC}"

# Clean and create testdata directory
rm -rf "$TESTDATA_DIR"
mkdir -p "$TESTDATA_DIR"

# Function to ensure pack files exist (only when there are objects)
ensure_pack_files() {
  local repo_path=$1
  local force_empty=${2:-false}
  echo -e "  ${BLUE}Ensuring pack files for $(basename $repo_path)...${NC}"

  # For truly empty repos, don't force pack creation
  if [ "$force_empty" = "true" ]; then
    echo -e "    ${GREEN}✓ Empty repository (no pack files needed)${NC}"
    return 0
  fi

  # Check if there are any loose objects or existing packs
  local loose_count=$(find "$repo_path/objects" -type f -path "*/[0-9a-f][0-9a-f]/[0-9a-f]*" 2>/dev/null | wc -l)
  local pack_count=$(find "$repo_path/objects/pack" -name "*.pack" 2>/dev/null | wc -l)

  if [ "$loose_count" -eq 0 ] && [ "$pack_count" -eq 0 ]; then
    echo -e "    ${RED}✗ No objects found to pack${NC}"
    return 1
  fi

  # If we already have pack files, we're done
  if [ "$pack_count" -gt 0 ]; then
    echo -e "    ${GREEN}✓ Already has $pack_count pack file(s)${NC}"
    return 0
  fi

  echo -e "    ${BLUE}Found $loose_count loose objects, creating pack files...${NC}"

  # Create pack directory if it doesn't exist
  mkdir -p "$repo_path/objects/pack"

  # Try different packing strategies
  git -C "$repo_path" repack -ad 2>/dev/null ||
    git -C "$repo_path" repack -a 2>/dev/null ||
    git -C "$repo_path" gc --aggressive 2>/dev/null || true

  # Final check
  pack_count=$(find "$repo_path/objects/pack" -name "*.pack" 2>/dev/null | wc -l)
  idx_count=$(find "$repo_path/objects/pack" -name "*.idx" 2>/dev/null | wc -l)

  if [ "$pack_count" -gt 0 ] && [ "$pack_count" -eq "$idx_count" ]; then
    echo -e "    ${GREEN}✓ Created $pack_count pack file(s)${NC}"
    return 0
  else
    echo -e "    ${RED}✗ Failed to create pack files (pack=$pack_count, idx=$idx_count)${NC}"
    return 1
  fi
}

# 1. Truly empty repository (no commits, no objects)
echo -e "${GREEN}Creating empty repository...${NC}"
REPO_PATH="$TESTDATA_DIR/empty-repo"

# Create a bare repository with no commits
git init --bare "$REPO_PATH" >/dev/null 2>&1

# Set up basic config
git -C "$REPO_PATH" config user.name "Test User"
git -C "$REPO_PATH" config user.email "test@example.com"

# Don't create any commits or objects - keep it truly empty
ensure_pack_files "$REPO_PATH" true

# 2. Simple linear repository
echo -e "${GREEN}Creating simple-linear repository...${NC}"
REPO_PATH="$TESTDATA_DIR/simple-linear"
WORK_DIR="$REPO_PATH.work"

git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"

# Commit 1
echo "# Test Repository" >"$WORK_DIR/README.md"
git -C "$WORK_DIR" add README.md
git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1

# Commit 2
cat >"$WORK_DIR/main.go" <<'EOF'
package main

func main() {
    println("Hello, World!")
}
EOF
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Add main.go" >/dev/null 2>&1

# Commit 3
echo "test content" >"$WORK_DIR/test.txt"
git -C "$WORK_DIR" add test.txt
git -C "$WORK_DIR" commit -m "Add test file" >/dev/null 2>&1

# Clone as bare
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"

ensure_pack_files "$REPO_PATH"

# Generate commit-graph
git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || true

# 3. Repository with merges
echo -e "${GREEN}Creating repository with merges...${NC}"
REPO_PATH="$TESTDATA_DIR/with-merges"
WORK_DIR="$REPO_PATH.work"

git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"

# Initial commit on main
echo "package main" >"$WORK_DIR/main.go"
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1

# Create and switch to feature branch
git -C "$WORK_DIR" checkout -b feature >/dev/null 2>&1

# Feature branch commit
cat >"$WORK_DIR/feature.go" <<'EOF'
package main

func feature() {
    println("Feature")
}
EOF
git -C "$WORK_DIR" add feature.go
git -C "$WORK_DIR" commit -m "Add feature" >/dev/null 2>&1

# Back to main branch
git -C "$WORK_DIR" checkout main >/dev/null 2>&1 || git -C "$WORK_DIR" checkout master >/dev/null 2>&1

# Main branch commit
echo -e "package main\n\nfunc main() {\n    println(\"Updated\")\n}" >"$WORK_DIR/main.go"
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Update main" >/dev/null 2>&1

# Merge feature branch
git -C "$WORK_DIR" merge feature -m "Merge feature branch" --no-ff >/dev/null 2>&1

# Post-merge commit
echo "After merge" >"$WORK_DIR/after_merge.txt"
git -C "$WORK_DIR" add after_merge.txt
git -C "$WORK_DIR" commit -m "Post-merge commit" >/dev/null 2>&1

# Clone as bare
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"

ensure_pack_files "$REPO_PATH"
git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || true

# 4. Repository without commit-graph
echo -e "${GREEN}Creating repository without commit-graph...${NC}"
REPO_PATH="$TESTDATA_DIR/no-commit-graph"
WORK_DIR="$REPO_PATH.work"

git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"

# Set a fixed date for deterministic commits
export GIT_AUTHOR_DATE="2023-01-01T12:00:00Z"
export GIT_COMMITTER_DATE="2023-01-01T12:00:00Z"

# Create 4 commits with substantial content
echo "# Project README" >"$WORK_DIR/README.md"
git -C "$WORK_DIR" add README.md
git -C "$WORK_DIR" commit -m "Initial commit - add README" >/dev/null 2>&1

echo "First file content line 1" >"$WORK_DIR/file1.txt"
echo "First file content line 2" >>"$WORK_DIR/file1.txt"
git -C "$WORK_DIR" add file1.txt
git -C "$WORK_DIR" commit -m "Add file1.txt with multiple lines" >/dev/null 2>&1

echo "Second file content line 1" >"$WORK_DIR/file2.txt"
echo "Second file content line 2" >>"$WORK_DIR/file2.txt"
git -C "$WORK_DIR" add file2.txt
git -C "$WORK_DIR" commit -m "Add file2.txt with multiple lines" >/dev/null 2>&1

echo "Third file content line 1" >"$WORK_DIR/file3.txt"
echo "Third file content line 2" >>"$WORK_DIR/file3.txt"
git -C "$WORK_DIR" add file3.txt
git -C "$WORK_DIR" commit -m "Add file3.txt with multiple lines" >/dev/null 2>&1

# Unset the fixed dates
unset GIT_AUTHOR_DATE
unset GIT_COMMITTER_DATE

# Clone as bare
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"

ensure_pack_files "$REPO_PATH"

# Remove commit-graph files to force packfile scanning
rm -f "$REPO_PATH/objects/info/commit-graph" 2>/dev/null
rm -rf "$REPO_PATH/objects/info/commit-graphs" 2>/dev/null

# Verify the commits were created properly
echo -e "  ${BLUE}Verifying commits in no-commit-graph repo...${NC}"
commit_count=$(git -C "$REPO_PATH" rev-list --all --count 2>/dev/null || echo "0")
if [ "$commit_count" -ne 4 ]; then
  echo -e "  ${RED}✗ Expected 4 commits, got $commit_count${NC}"
else
  echo -e "  ${GREEN}✓ Created 4 commits as expected${NC}"
fi

# 5. Large repository
echo -e "${GREEN}Creating large repository (100 commits)...${NC}"
REPO_PATH="$TESTDATA_DIR/large-repo"
WORK_DIR="$REPO_PATH.work"

git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"

# Initial commit
echo "# Large Repository" >"$WORK_DIR/README.md"
git -C "$WORK_DIR" add README.md
git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1

# Create 99 more commits (total 100)
for i in $(seq 2 100); do
  echo "Content for file $i" >"$WORK_DIR/file_$i.txt"
  git -C "$WORK_DIR" add "file_$i.txt"
  git -C "$WORK_DIR" commit -m "Add file_$i.txt (commit $i)" >/dev/null 2>&1

  # Progress indicator
  if [ $((i % 25)) -eq 0 ]; then
    echo -e "  Created $i/100 commits..."
  fi
done

# Clone as bare
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"

ensure_pack_files "$REPO_PATH"
git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || true

# Final verification
echo -e "\n${BLUE}Test data generation complete!${NC}"
echo -e "${BLUE}Verifying repositories...${NC}"
echo -e "${BLUE}═══════════════════════════════════════${NC}"

all_good=true
for repo in "$TESTDATA_DIR"/*; do
  if [ -d "$repo" ]; then
    repo_name=$(basename "$repo")
    pack_count=$(find "$repo/objects/pack" -name "*.pack" 2>/dev/null | wc -l)
    idx_count=$(find "$repo/objects/pack" -name "*.idx" 2>/dev/null | wc -l)

    # Special handling for empty repo
    if [ "$repo_name" = "empty-repo" ]; then
      if [ "$pack_count" -eq 0 ]; then
        echo -e "  ${GREEN}✓${NC} $repo_name: empty (no pack files)"
      else
        echo -e "  ${RED}✗${NC} $repo_name: should be empty but has $pack_count pack files"
        all_good=false
      fi
    else
      if [ "$pack_count" -gt 0 ] && [ "$pack_count" -eq "$idx_count" ]; then
        echo -e "  ${GREEN}✓${NC} $repo_name: $pack_count pack file(s)"
      else
        echo -e "  ${RED}✗${NC} $repo_name: pack=$pack_count, idx=$idx_count"
        all_good=false
      fi
    fi
  fi
done

if $all_good; then
  echo -e "\n${GREEN}All repositories created successfully!${NC}"

  # Additional verification: check commit counts for debugging
  echo -e "\n${BLUE}Repository commit counts (for verification):${NC}"
  for repo in "$TESTDATA_DIR"/*; do
    if [ -d "$repo" ]; then
      repo_name=$(basename "$repo")
      if [ "$repo_name" = "empty-repo" ]; then
        echo -e "  $repo_name: 0 commits (expected)"
      else
        commit_count=$(git -C "$repo" rev-list --all --count 2>/dev/null || echo "0")
        echo -e "  $repo_name: $commit_count commits"
      fi
    fi
  done

  exit 0
else
  echo -e "\n${RED}Some repositories failed validation!${NC}"
  exit 1
fi
