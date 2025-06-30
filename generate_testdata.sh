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

  # If we already have pack files but also have loose objects, force repack
  if [ "$pack_count" -gt 0 ] && [ "$loose_count" -gt 0 ]; then
    echo -e "    ${BLUE}Found $pack_count existing pack(s) and $loose_count loose objects, repacking...${NC}"
    git -C "$repo_path" repack -ad >/dev/null 2>&1 || git -C "$repo_path" repack -a >/dev/null 2>&1
  elif [ "$pack_count" -gt 0 ]; then
    echo -e "    ${GREEN}✓ Already has $pack_count pack file(s)${NC}"
    return 0
  elif [ "$loose_count" -eq 0 ]; then
    echo -e "    ${RED}✗ No objects found to pack${NC}"
    return 1
  else
    echo -e "    ${BLUE}Found $loose_count loose objects, creating pack files...${NC}"
    git -C "$repo_path" repack -ad >/dev/null 2>&1 ||
      git -C "$repo_path" repack -a >/dev/null 2>&1 ||
      git -C "$repo_path" gc --aggressive >/dev/null 2>&1
  fi

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

# --- Start of Repository Generation ---

# 1. Truly empty repository (no commits, no objects)
echo -e "\n${GREEN}1. Creating empty repository...${NC}"
REPO_PATH="$TESTDATA_DIR/empty-repo"
git init --bare "$REPO_PATH" >/dev/null 2>&1
git -C "$REPO_PATH" config user.name "Test User"
git -C "$REPO_PATH" config user.email "test@example.com"
ensure_pack_files "$REPO_PATH" true

# 2. Simple linear repository
echo -e "\n${GREEN}2. Creating simple-linear repository...${NC}"
REPO_PATH="$TESTDATA_DIR/simple-linear"
WORK_DIR="$REPO_PATH.work"
git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"
echo "# Test Repository" >"$WORK_DIR/README.md"
git -C "$WORK_DIR" add README.md
git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1
cat >"$WORK_DIR/main.go" <<'EOF'
package main
func main() {
    println("Hello, World!")
}
EOF
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Add main.go" >/dev/null 2>&1
echo "test content" >"$WORK_DIR/test.txt"
git -C "$WORK_DIR" add test.txt
git -C "$WORK_DIR" commit -m "Add test file" >/dev/null 2>&1
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"
ensure_pack_files "$REPO_PATH"
git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || true

# 3. Repository with merges
echo -e "\n${GREEN}3. Creating repository with merges...${NC}"
REPO_PATH="$TESTDATA_DIR/with-merges"
WORK_DIR="$REPO_PATH.work"
git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"
echo "package main" >"$WORK_DIR/main.go"
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1
git -C "$WORK_DIR" checkout -b feature >/dev/null 2>&1
cat >"$WORK_DIR/feature.go" <<'EOF'
package main
func feature() {
    println("Feature")
}
EOF
git -C "$WORK_DIR" add feature.go
git -C "$WORK_DIR" commit -m "Add feature" >/dev/null 2>&1
git -C "$WORK_DIR" checkout main >/dev/null 2>&1 || git -C "$WORK_DIR" checkout master >/dev/null 2>&1
echo -e "package main\n\nfunc main() {\n    println(\"Updated\")\n}" >"$WORK_DIR/main.go"
git -C "$WORK_DIR" add main.go
git -C "$WORK_DIR" commit -m "Update main" >/dev/null 2>&1
git -C "$WORK_DIR" merge feature -m "Merge feature branch" --no-ff >/dev/null 2>&1
echo "After merge" >"$WORK_DIR/after_merge.txt"
git -C "$WORK_DIR" add after_merge.txt
git -C "$WORK_DIR" commit -m "Post-merge commit" >/dev/null 2>&1
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"
ensure_pack_files "$REPO_PATH"
git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || true

# 4. Repository without commit-graph
echo -e "\n${GREEN}4. Creating repository without commit-graph...${NC}"
REPO_PATH="$TESTDATA_DIR/no-commit-graph"
WORK_DIR="$REPO_PATH.work"
git init "$WORK_DIR" >/dev/null 2>&1
git -C "$WORK_DIR" config user.name "Test User"
git -C "$WORK_DIR" config user.email "test@example.com"
export GIT_AUTHOR_DATE="2023-01-01T12:00:00Z"
export GIT_COMMITTER_DATE="2023-01-01T12:00:00Z"
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
unset GIT_AUTHOR_DATE
unset GIT_COMMITTER_DATE
git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
rm -rf "$WORK_DIR"
ensure_pack_files "$REPO_PATH"
rm -f "$REPO_PATH/objects/info/commit-graph" 2>/dev/null
rm -rf "$REPO_PATH/objects/info/commit-graphs" 2>/dev/null
echo -e "  ${BLUE}Verifying commits in no-commit-graph repo...${NC}"
commit_count=$(git -C "$REPO_PATH" rev-list --all --count 2>/dev/null || echo "0")
if [ "$commit_count" -ne 4 ]; then
  echo -e "    ${RED}✗ Expected 4 commits, got $commit_count${NC}"
else
  echo -e "    ${GREEN}✓ Created 4 commits as expected${NC}"
fi

# ==============================================================================
# Reusable function to generate large repositories with a linear history
# ==============================================================================
# Arguments:
#   $1: The base name for the repository (e.g., "large-repo")
#   $2: The total number of commits to create.
generate_large_linear_repo() {
  local repo_name="$1"
  local commit_count="$2"

  echo -e "\n${GREEN}Creating $repo_name repository with $commit_count commits...${NC}"
  local REPO_PATH="$TESTDATA_DIR/$repo_name"
  local WORK_DIR="$REPO_PATH.work"

  git init "$WORK_DIR" >/dev/null 2>&1
  git -C "$WORK_DIR" config user.name "Test User"
  git -C "$WORK_DIR" config user.email "test@example.com"

  # Initial commit
  echo "# $repo_name" >"$WORK_DIR/README.md"
  git -C "$WORK_DIR" add README.md
  git -C "$WORK_DIR" commit -m "Initial commit" >/dev/null 2>&1

  # Determine progress reporting step (report ~10 times)
  local progress_step=$((commit_count / 10))
  [ "$progress_step" -eq 0 ] && progress_step=1 # Avoid division by zero for small counts

  # Create remaining commits
  for i in $(seq 2 "$commit_count"); do
    echo "Content for file $i in $repo_name" >"$WORK_DIR/file_$i.txt"
    git -C "$WORK_DIR" add "file_$i.txt"
    git -C "$WORK_DIR" commit -m "Add file_$i.txt (commit $i of $commit_count)" >/dev/null 2>&1

    # Progress indicator
    if [ $((i % progress_step)) -eq 0 ]; then
      echo -e "  Created $i/$commit_count commits..."
    fi
  done
  echo -e "  Created $commit_count/$commit_count commits..." # Final progress update

  # Clone as bare - this ensures all objects are properly transferred
  echo -e "  ${BLUE}Cloning to bare repository...${NC}"
  git clone --bare "$WORK_DIR" "$REPO_PATH" >/dev/null 2>&1
  rm -rf "$WORK_DIR"

  # Force pack file recreation and verification
  ensure_pack_files "$REPO_PATH"

  # Generate commit-graph AFTER packing to ensure consistency
  echo -e "  ${BLUE}Generating commit-graph...${NC}"
  git -C "$REPO_PATH" commit-graph write --reachable 2>/dev/null || {
    echo -e "    ${RED}✗ Failed to create commit-graph${NC}"
    return 1
  }
  echo -e "    ${GREEN}✓ Commit-graph created successfully${NC}"
}

# 5. Large repository (100 commits)
generate_large_linear_repo "large-repo" 100

# 6. Very large repository (1,000 commits)
generate_large_linear_repo "very-large-repo-1k" 1000

# 7. Super large repository (10,000 commits)
# This may take a few minutes to generate depending on your machine.
generate_large_linear_repo "super-large-repo-10k" 10000

# --- Final Verification ---
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
