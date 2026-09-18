# What the vector capability costs a write-heavy table

ETP-5118 asks whether the change capture introduced by the vector feature regresses writes on busy
tables, and whether the feature is genuinely zero-cost while it is off. This is the measurement
that answers it, and the method, so it can be repeated rather than believed.

## Where the cost can come from

Only two things in this module can attach themselves to a write on a business table:

- the generated `etarc_vsrc_*` triggers, and
- a Java persistence observer.

There is no observer on business entities: `PartitionTableEventHandler` watches `ETARC_Table_Config`
and `VectorSourceConfigurationEventHandler` watches `ETARC_Vector_Source` and its columns, both of
them configuration tables. So the whole question is the triggers, and a trigger exists only for a
source the dictionary considers ready — active, enabled, with a key column, a provider and at least
one content column.

A table with no such source therefore carries nothing at all. That is not a small overhead to be
measured; it is the absence of an object. In the instance this was measured on, three sources were
configured and exactly three tables carried triggers.

## Method

Each scenario runs in its own transaction and is rolled back, so every one starts from the same
table state and nothing persists. Timing comes from `clock_timestamp()` around 40 iterations over
the 85 rows of `M_Product` — 3,400 row writes — after 5 warm-up iterations inside the same
transaction. `ALTER TABLE ... DISABLE TRIGGER` is transactional in PostgreSQL, which is what lets
the same table be measured with and without the module's triggers without changing anything.

Only the module's own triggers are disabled. Audit and business triggers fire in every scenario, so
what the comparison isolates is this module's share.

> **A first attempt ran all the scenarios inside one transaction and produced nonsense** — the
> baseline came out three times slower than the instrumented case. Each iteration leaves 85 dead
> tuples behind, so by the last scenario the table carried some forty thousand of them and the
> measurement was reading the order of execution rather than the triggers. Anyone repeating this
> should keep the scenarios in separate transactions.

## Results

Microseconds per row written, two independent repetitions:

| Scenario | Triggers | rep 1 | rep 2 |
|---|---|---:|---:|
| A — watched column, value really changes | on | 416.7 | 436.1 |
| D — same write as A | **off** | 289.5 | 310.4 |
| C — column nobody watches | on | 305.1 | 311.5 |
| E — same write as C | **off** | 299.3 | 313.3 |
| B — watched column, written with its own value | on | 303.4 | 325.4 |

## What it means

**Off: nothing.** No trigger, no observer, no object. Not a measured near-zero — an absence.

**On, writing a column nobody watches: nothing measurable.** C against E is under one percent, well
inside the noise between repetitions. This is `AFTER UPDATE OF <column>` earning its place: the
trigger is not considered at all, so a table indexed on `name` pays nothing when a process updates
stock, prices or dates.

**On, writing a watched column with the value it already had: a few percent.** B sits just above the
baseline, which is the cost of evaluating `OLD.col IS DISTINCT FROM NEW.col` and finding it false.
Nothing is enqueued. This matters more than it looks, because a save that rewrites every field is
the common shape in an ERP.

**On, actually changing a watched column: about 40% on that statement.** A against D is roughly
130 µs per row, which is one INSERT into the outbox. This is the real price of the feature, and it
is paid only by writes that genuinely change indexed content.

So the exposure of a write-heavy table depends on what it writes, not on how much: a table whose
watched columns are descriptive and rarely touched sits in scenario C, and a process that rewrites
the indexed text of every row on every pass sits in scenario A and pays for it.

## Limits of this measurement

Eighty-five rows written repeatedly, one connection, no concurrency, a development container. The
relative differences are what carry over; the absolute microseconds do not. Nothing here measures
the outbox drain or the embedding calls, which are asynchronous and paid by the scheduled process
rather than by the write.
