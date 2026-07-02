## Documentation and Commenting Style

All documentation for this codebase exists in the actual code as a rustdoc, with only a minimal `README.md` that shouldn't need to evolve very much (if at all) over time. Documentation, just like code, should have good abstractions. Some general rules:

- A piece of information should exist as a comment in exactly one place, and referenced from other places.
- Variable names should be self-descriptive as much as possible, to avoid needing a comment to begin with.
- If a human said something to you in the way as for reasoning for a change, it's probably a good idea to write down in one place, minimally, as close to the way they said it as possible.

Comments should explain specific constraints that were part of the spec for the software.

Example (cartoonishly BAD, do not do this):
```rust
// user id stores a unique random id (UUIDv7) for each user
let user_id = Uuidv7::generate();
```

Example (GOOD, do this):
```rust
let user_id = Uuidv7::generate();
```

Example (GOOD, do this):
```rust
// using v7 for db performance reasons: <reference to some issue or thread maybe>
let user_id = Uuidv7::generate();
```

**If unsure**: it is better to leave no comment or doc than one that is useless, as it makes the code harder for humans to understand and review.

## Golden Rule

Existing code or documentation in this codebase may not follow the above rules. That is not an excuse to add new things that also don't follow the rules. Leave the codebase a better place than you found it!
