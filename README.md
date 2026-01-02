# Cecs 🍪

Cecs, pronounced \[ˈkɛks\] is an ECS implementation supporting Cao-Lo.

There are many like it, but this one is mine.

Heavily inspired by [Bevy](https://bevyengine.org/) and [Hexops](https://devlog.hexops.com/2022/lets-build-ecs-part-2-databases/)

## Features

- Functions as systems
- Query interface
- Unique data, called Resources
- Cloning of the entire database (optional)
- Serialization/Deserialization of select _components_ (optional)
- Work-stealing based parallel scheduler (optional)
- "View" systems. Allows running systems that only read data on an immutable reference to the World.
- Explicit ordering between systems in the same stage.
- Nested stages
