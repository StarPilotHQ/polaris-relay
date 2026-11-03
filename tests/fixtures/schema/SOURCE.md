# Vendored capnp schemas

Copied unmodified from StarPilot, for the tests' synthetic qlogs; at runtime Polaris Relay uses the schema bundle the
site serves (`GET /api/v1/schema`). StarPilot's Event
union is wire-identical to openpilot master (same ordinals and discriminants) and adds StarPilot's
own messages in the fork-reserved slots, so these schemas read stock openpilot routes too; the
custom-slot messages are only kept for routes detected as StarPilot.

| File | Source | Commit |
|---|---|---|
| `log.capnp`, `custom.capnp`, `deprecated.capnp`, `include/c++.capnp` | firestar5683/StarPilot `openpilot/cereal/` | `cafb6df8c2eaa1d22252b9dafe6142b52eed3ec4` (branch `SecretGoodStarPilot`, 2026-10-25) |
| `car.capnp` | firestar5683/StarPilot `opendbc_repo/opendbc/car/car.capnp` | same commit |

StarPilot's `car.capnp` differs from upstream opendbc at some ordinals (e.g. `rotationalInertia`
where upstream has `rotationalInertiaPerMass`), so all files must come from the same StarPilot
commit. Both projects are MIT licensed.

To update: copy the same files from a newer commit, update this table, and run `pytest`. Every
accepted clip ships with the exact schema files that encoded it (`schemas/<scrubber_version>/`).
