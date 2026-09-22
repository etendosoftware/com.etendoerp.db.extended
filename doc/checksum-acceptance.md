# Why the vector objects are created by the update

Everything this module creates in the database — the pgvector extension, the runtime storage, a
collection per search source and the change capture triggers — is created by the post-update module
script, inside `update.database`. Configuring a source is therefore two steps: save it, then run the
update.

That is not an arbitrary choice, and it was not the first one. This note records what was measured
on the way to it, so the decision is not re-litigated from memory.

## What the structure checksum covers

`ad_db_modified`, in `src-db/database/model/prescript-PostgreSql.sql`, hashes PL functions,
triggers, tables with their primary keys, columns, foreign keys and indexes, and materialized
views. **It hashes no data at all** — no `AD_*` row of any kind takes part. It does not read
`excludeFilter.xml` either: that file keeps objects out of the DBSM model comparison, which is a
different check.

Every one of its queries is restricted to `current_schema()` **except the one for triggers**, which
has no schema filter and excludes only names matching `'^RI'` or `UPPER(name) LIKE 'AU_%'` — note
that `_` is a wildcard in `LIKE`.

Measured on PostgreSQL 16, each inside a transaction that was rolled back, calling only
`ad_db_modified('N')`, which does not write:

| object created | verdict |
| --- | --- |
| table in another schema | `N` — invisible |
| table in the application schema | `Y` |
| function in another schema | `N` — invisible |
| function in the application schema | `Y` |
| trigger, ordinary name, function in the application schema | `Y` |
| trigger, ordinary name, function in another schema | `Y` |
| trigger named `AU_…` | `N` |

So a trigger escapes the checksum only by its name, because the trigger's function body is hashed
as part of the trigger row. Keeping the storage in its own schema takes it out; the capture
triggers cannot be taken out, because they belong to the tables they watch.

## Why that settles where the work happens

Creating those triggers moves the checksum. `update.database` then refuses to run:
`DBUpdater.checkIfDBWasModified` aborts with *Database has local changes. Update.database will not
be done.* before reaching the end, where the core re-stamps.

A window action can only get past that by accepting the structure itself — and the checksum is a
single MD5 of the whole schema, with no way to attribute a delta. Accepting the triggers therefore
also accepts every other change present at that moment, including dictionary work somebody had not
yet exported. On an instance many people share, that is a lot of authority for a button.

Run from inside the update, the question disappears rather than being managed: the run that creates
the objects is the run that accepts the structure, and the core already re-stamps once every
post-update script has finished (EPL-1810).

## What this costs

A new search source does not take effect when it is saved. It takes effect on the next
`update.database`. The window reports whether a source is going to produce anything, so that is
answerable before running it, but applying it is a deploy-time act.

## Rejected alternatives

- **Accepting the structure from the button, guarded.** This is what the module did first. The
  guard grew to three conditions — a checksum had to exist, it had to have been accepted
  beforehand, and the structure had to have actually moved — because `ad_db_modified` answers `N`
  both when the stored checksum matches and when there is none (`aux is null or aux = computed`),
  and ends in `EXCEPTION WHEN OTHERS THEN RETURN 'N'`, so an unguarded reading cannot tell a clean
  database from one that cannot answer. Even fully guarded it still accepted a concurrent third
  party's DDL, which no amount of care inside the module can prevent.
- **An advisory lock around the activation.** Session-scoped while the run spans several
  transactions: the checkpoint closes the connection, which returns to the pool still holding the
  lock. It also only serialised activations against each other, which is the harmless case.
- **Naming the module's triggers `AU_…`.** Measured to work. It squats on the core's audit prefix,
  states something untrue about the objects, and depends on an undocumented pattern: if the core
  ever tightens it, the regression is silent and surfaces on a customer instance as a refused
  `update.database`.
- **A plain `ModuleScript` instead of a post-update one.** Tried, and wrong. In `DBUpdater` the
  order is `executeModuleScripts` and only then `Platform.alterData`, which is where the sourcedata
  is applied, so a plain script generates the triggers of a source from the configuration the
  update is about to replace — wrong precisely for the sources a module ships.

## The extension is the one thing left that moves the checksum

Measured on this database: dropping the `vector` extension takes the verdict from `Y` to `N`, and
putting it back takes it to `Y`. pgvector installs 118 functions; 114 are C functions, which the
hash skips because it only counts those with `probin IS NULL`. The other four are the `avg` and
`sum` aggregates for `vector` and `halfvec`, they are SQL, they land in the application schema, and
they are hashed.

That is harmless when the post-update script creates the extension, because the core re-stamps the
checksum once every post-update script has run, so the update that creates it also accepts it.

It is not harmless when a DBA creates it out of band -- which is the common case, because the
application role usually may not `CREATE EXTENSION`. The database is then left reporting local
changes and the next `update.database` refuses to start. The way through is one forced update: the
delta really is only the extension, and that same update re-stamps the checksum at its end, so it
is needed once and not again.

**Worth considering:** `CREATE EXTENSION vector SCHEMA etarc_vector` would put those four
aggregates outside `current_schema()` and make even the out-of-band case invisible. It would mean
qualifying the vector type and the distance operators everywhere they are used, or setting a
`search_path`, so it is a change to weigh rather than an obvious win.

## Still open

`PostUpdateModuleScript` lives only on the core's `epic/ETP-3504` (EPL-1810), not on `develop` or
`main`, and this module depends on it. Until it reaches a released core, this module cannot be
installed on one: `ModuleScriptHandler` resolves every module script class, a missing superclass
raises `NoClassDefFoundError`, and an `Error` escapes its catch for `Exception`, so the whole
install fails rather than this one script. The pipeline pins the core branch for the same reason.
