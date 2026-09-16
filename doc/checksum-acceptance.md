# Accepting the database structure after activation

Activating a vector source installs database objects while the application is running. That moves
the structure checksum, and the next `update.database` refuses to run until somebody accepts it.
This note records what was measured, what the module does about it today, and what the core would
have to offer for the module to stop doing it at all.

It is written for a decision, not as a description of the code.

## What the checksum actually covers

`ad_db_modified`, in `src-db/database/model/prescript-PostgreSql.sql`, hashes PL functions,
triggers, tables with their primary keys, columns, foreign keys and indexes, and materialized
views. **It hashes no data at all** — no `AD_*` row of any kind takes part. It does not read
`excludeFilter.xml` either: that file keeps objects out of the DBSM model comparison, which is a
different check.

Every one of its queries is restricted to `current_schema()` **except the one for triggers**, which
has no schema filter and excludes only names matching `'^RI'` or `UPPER(name) LIKE 'AU_%'` — note
that `_` is a wildcard in `LIKE`, so that pattern is `AU` followed by any character.

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

Two consequences. Storage kept in its own schema is not part of the checksum. And a trigger escapes
only by its name, because the trigger's function body is hashed as part of the trigger row.

## What the module does today

The runtime storage lives in the `etarc_vector` schema, so creating it is not a local change anybody
has to accept. Tables left in the application schema by an earlier version are moved there on the
next activation, carrying their rows, indexes and foreign key.

What remains is the capture triggers, which belong to the tables they watch. For those the
activation re-stamps the checksum, under three conditions:

- a checksum is stored at all. `ad_db_modified` answers `N` both when the stored checksum matches
  and when there is none — its test is `aux is null or aux = computed` — so a database restored
  from a dump without one reports itself unmodified;
- it answered `N` before the run touched anything;
- it answers `Y` afterwards. Having just installed triggers, `N` can only mean the function is not
  answering: it ends in `EXCEPTION WHEN OTHERS THEN RETURN 'N'`.

The acceptance is logged with the checksum it replaced and the one it stamped.

The `update.database` path needs none of this. DBSM runs module scripts before stamping
(`executeModuleScripts` precedes `updateCRC` in `DBUpdater`), so triggers generated there are
accepted by the same update that generated them.

## What is left, and what the core could do

The window the module cannot close: between reading the baseline and stamping, DDL from somebody
else can land and be accepted along with ours. It is narrow — `update.database` needs Tomcat down
and the button needs it up, so the realistic source is manual DDL on a live instance — but the
checksum is a single MD5 of the whole schema, with no way to attribute a delta, so no amount of
care inside the module closes it.

**Proposal.** A declarative exclusion, owned by a module and constrained to its own objects. A
dictionary table of module, object type and name prefix, and one clause in the trigger loop:

```sql
AND NOT EXISTS (SELECT 1 FROM ad_db_checksum_exclusion x
                 WHERE x.isactive = 'Y' AND x.object_type = 'TRIGGER'
                   AND upper(trg.tgname) LIKE upper(x.name_prefix) || '%')
```

What keeps it from leaking is a single rule: `name_prefix` must begin with an `AD_MODULE_DBPREFIX`
belonging to that module. This module's prefix is `ETARC` and its triggers are named
`etarc_vsrc_…`, so it fits; and it structurally cannot exclude `AD_*`, `C_*`, or another module's
objects. The rows are dictionary data, exported with the module and reviewed in its pull request —
unlike `AU_%`, where the only approval needed is choosing a name.

It should cover functions as well as triggers. A trigger's function is already left out while the
trigger exists, since the function loop skips anything in `pg_trigger`, but a function briefly
outliving its trigger would reappear in the hash.

With that in place the module stops re-stamping entirely: an object that never enters the hash
moves it neither when created nor when dropped, so the acceptance and its guard are deleted rather
than maintained.

Two things worth knowing before writing it:

- **It deploys by itself.** The prescript runs on `update.database` as well as on create
  (`DBUpdater.executePreScript`), so no migration script is needed.
- **It is invisible to itself.** The function loop excludes anything named `ad_db_modified`, so
  changing that function does not move the checksum. Verified by measurement: adding an overload of
  that name leaves the verdict at `N`, while any other new function moves it to `Y`. The core change
  would not make every existing instance report local changes.
- **The bootstrap needs a guard.** The prescript runs before the model is updated, so on the first
  update against the new core the exclusion table does not exist yet. The query would fail, and
  because `ad_db_modified` ends in `EXCEPTION WHEN OTHERS THEN RETURN 'N'` it would answer "no
  changes" rather than failing loudly. Guard it with
  `to_regclass('ad_db_checksum_exclusion') IS NOT NULL`, or the fix introduces a worse failure than
  the one it removes.

## Rejected alternatives

- **Naming the module's triggers `AU_…`.** Works, and was measured to work. It squats on the core's
  audit prefix, states something untrue about the objects, and depends on an undocumented pattern:
  if the core ever tightens it, the regression is silent and surfaces on a customer instance as a
  refused `update.database`.
- **Not stamping, and letting the next `update.database` do it.** It cannot. `checkIfDBWasModified`
  aborts with "Database has local changes. Update.database will not be done." before reaching the
  end, where EPL-1810 re-stamps.
- **Passing the expected objects to `ad_db_modified` so it can attribute the delta.** Precise for
  objects that appear, but not symmetric: tearing a source down removes triggers the stored checksum
  still accounts for, so the comparison fails and the acceptance is refused. A declarative exclusion
  has no direction to get wrong.
- **An advisory lock around the activation.** Tried and removed. It is session-scoped while the run
  spans several transactions: the checkpoint closes the connection, which returns to the pool still
  holding the lock. It also only serialised activations against each other, which is the case that
  does no harm.
