Changelog
## [0.1.9] - 2026-01-03

### 🚀 Features

- Add stage nesting

### 🐛 Bug Fixes

- Fix sparse saves.

### 🚜 Refactor

- [**breaking**] Run all should_run systems, even if one fails the check

### ⚙️ Miscellaneous Tasks

- Update dependencies

## [0.1.8] - 2025-09-26

### 🚀 Features

- Accept SystemStageBuilder in add_|run_stage
- Abort if a worker thread panics.
- Add versioning to WorldPersister

### 🚜 Refactor

- [**breaking**] Rename persister module to serde

### ⚙️ Miscellaneous Tasks

- Skip serializing empty component lists
- Update to 2024 edition

## [0.1.7] - 2025-07-18

### 🚀 Features

- `World::insert_id` allows inserting pre-allocated entityids
- Queries can take nested tuples
- Expose the WorldQuery trait
- Allow inserting premade ids via Commands
- Add JobPool::new
- () is now a WorldQuery
- Allow packing queries into tuples
- Allow extending queries with additional filters
- Allow querying mutable and immutable reference to the same type in different queries within the same QuerySet

### 🐛 Bug Fixes

- Fix read_only property of tuple queries
- Fix system ordering
- Fix compile error in rust 1.90
- Fix warning when tracing feature is disabled

### 🚜 Refactor

- Use Arc instead of Rc inside SystemDescriptors as they are Send
- [**breaking**] Remove tuple implementation for 16+ length tuples, as tuples may be nested now
- Reduce number of 'spins' the job system threads do when the queues are empty
- Add SystemStageBuilder
- Use the same formatting for EntityId in debug and display
- Reduce monomorphisation

### ⚙️ Miscellaneous Tasks

- Add len and is_empty to SystemStage
- Retain the commands buffer.
- Update deps

## [0.1.6] - 2024-08-06

### 🚀 Features

- Implement future for JobHandle
- Add `World::get_or_insert_resource`
- Differentiate beween the main thread and foreign threads. Using multiple JobPools will no longer block the main thread

### 🐛 Bug Fixes

- Fix panic if the commands payload doesn't contain entity commands
- Fix memory leak in JobSystem when executing futures

### ⚙️ Miscellaneous Tasks

- Fix unused warning for QuerySets

## [0.1.5] - 2024-07-19

### 🚀 Features

- Use deterministic command execution
- Preserve EntityIds when deserializing a World
- Allow users to replace the job_system of a World
- Add vacuum method to the database
- Add IntoOnceSystem
- Query::subset
- Add Has<T> queries
- Query an entity's EntityTable
- Remove empty archetypes from the public interface
- Support running futures on the JobSystem

### 🐛 Bug Fixes

- Fix stage execution when feature: parallel is disabled
- Fix a bug where new archetypes would invalidate exiting ids
- Fix archetypes of bundles where the entity already has some of the bundle components

### 💼 Other

- Panic on incorrect Command use
- :clone

### 🚜 Refactor

- Invert if
- Return commands result in run_system
- [**breaking**] Remove system pipes
- [**breaking**] Remove component setters from the public interface

