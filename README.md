# go-gitpack

A minimal, memory-mapped Git object store that resolves objects directly from `*.pack` files without shelling out to the Git executable.

## Overview

The `objstore` package provides fast, read-only access to Git objects stored in packfiles. It's designed for scenarios where you need low-latency lookups, such as secret scanning, indexing, etc.

Note: This is very much experimental and a learning repo.
