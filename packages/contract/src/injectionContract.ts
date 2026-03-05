import type { ExecutionContext, QueryDefinition } from '@mkven/multi-db-validation'
import { ExecutionError, ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'
import type { QueryContract } from './queryContract.js'

// ── Config types ───────────────────────────────────────────────

/** Column reference, optionally qualified with a joined table. */
export interface ColRef {
  column: string
  /** Specify only when the column belongs to a joined table. */
  table?: string
}

/**
 * One query-routing target that exercises a single SQL generator path.
 *
 * Provide multiple targets to cover different SQL dialects / escaping
 * implementations (e.g. one per backend + one for the federation layer).
 */
export interface InjectionTarget {
  /** Human label for `describe()` output (e.g. `'orders'`, `'events+users'`). */
  label: string

  // ── Query routing ──────────────────────────────────────
  from: string
  join?: string
  /** Explicit SELECT columns (limits which columns appear in the result). */
  columns?: string[]

  // ── Available columns ──────────────────────────────────

  /** String column for `=`, `!=`, `in`, `like`, `contains`, etc. */
  string: ColRef
  /** Numeric column — `between`/`notBetween` injects a non-numeric string → expect `'rejected'`. */
  numeric?: ColRef
  /** Timestamp column — `between`/`notBetween` injects a malformed date → expect `'safe'`. */
  timestamp?: ColRef
  /** Array column for `arrayContains` / `arrayContainsAll` / `arrayContainsAny`. */
  array?: { column: string; expected?: 'escaped' | 'safe' }
  /** Column for `levenshteinLte`. */
  levenshtein?: ColRef

  // ── Aggregation (for alias injection tests) ────────────

  agg: ColRef & {
    fn: 'sum' | 'count'
    groupBy: string
  }

  // ── Miscellaneous ──────────────────────────────────────

  /** If set, tests `byIds` injection with this expected outcome. */
  byIds?: 'escaped' | 'safe'
  /** Expected outcome for null-byte injection (default `'escaped'`). */
  nullByteExpected?: 'escaped' | 'safe'
}

/**
 * Tables / columns used by the identifier & structural injection
 * tests (§ 16.1).  These tests verify the **validation layer**, not
 * SQL escaping, so only table/column names matter.
 */
export interface StructuralConfig {
  /** A table with at least: id, a string column, a numeric column, and a FK-like column. */
  table: string
  idColumn: string
  stringColumn: string
  numericColumn: string
  /** A foreign-key-like column on the same table (for refColumn filter test). */
  refColumn: string
  /** A second table (different from `table`). */
  secondTable: string
  /** Table used in EXISTS filters. */
  existsTable: string
  /** Cross-DB join pair for the cross-table column injection test. */
  joinFrom: string
  joinTo: string
}

export interface InjectionContractConfig {
  structural: StructuralConfig
  targets: InjectionTarget[]
}

// ── Helpers ────────────────────────────────────────────────────

const admin: ExecutionContext = { roles: { user: ['admin'] } }

/**
 * Assert that an injection attempt is neutralised.
 *
 * - `'escaped'` — the value was parameterized; the query **succeeds**.
 * - `'rejected'` — rejected with `ValidationError` or `ExecutionError`.
 * - `'safe'` — either outcome is acceptable (mode-dependent).
 */
async function expectInjectionSafe(
  engine: QueryContract,
  definition: QueryDefinition,
  expected: 'escaped' | 'rejected' | 'safe' = 'escaped',
): Promise<void> {
  try {
    const r = await engine.query({ definition, context: admin })
    if (expected === 'rejected') {
      expect.fail('Expected rejection (ValidationError | ExecutionError) but query succeeded')
    }
    expect(r.kind).toBeDefined()
  } catch (err) {
    if (expected === 'escaped') {
      expect.fail(
        `Expected query to succeed (value escaped) but got ${err instanceof Error ? err.constructor.name : typeof err}: ${String(err)}`,
      )
    }
    expect(
      err instanceof ValidationError || err instanceof ExecutionError,
      `Expected ValidationError | ExecutionError, got ${err instanceof Error ? err.constructor.name : typeof err}: ${String(err)}`,
    ).toBe(true)
  }
}

async function expectValidationError(engine: QueryContract, definition: QueryDefinition, code: string): Promise<void> {
  try {
    await engine.query({ definition, context: admin })
    expect.fail(`Expected ValidationError with code ${code}`)
  } catch (err) {
    expect(err).toBeInstanceOf(ValidationError)
    if (err instanceof ValidationError) {
      expect(err.errors.some((e) => e.code === code)).toBe(true)
    }
  }
}

// ── Payloads & operators ───────────────────────────────────────

const SQL = "'; DROP TABLE t; --"

const STRING_OPS: ReadonlyArray<{ op: string; value: string | string[] }> = [
  { op: '=', value: SQL },
  { op: '!=', value: SQL },
  { op: 'in', value: [`x${SQL}`] },
  { op: 'notIn', value: [`x${SQL}`] },
  { op: 'like', value: `%${SQL}%` },
  { op: 'notLike', value: `%${SQL}%` },
  { op: 'contains', value: SQL },
  { op: 'notContains', value: SQL },
  { op: 'icontains', value: SQL },
  { op: 'notIcontains', value: SQL },
  { op: 'ilike', value: `%${SQL}%` },
  { op: 'notIlike', value: `%${SQL}%` },
  { op: 'startsWith', value: SQL },
  { op: 'endsWith', value: SQL },
  { op: 'istartsWith', value: SQL },
  { op: 'iendsWith', value: SQL },
]

const ADVANCED_PAYLOADS: ReadonlyArray<{
  name: string
  value: string
  defaultExpected: 'escaped' | 'safe'
  nullByte?: true
}> = [
  { name: 'backslash-quote bypass', value: `\\'${SQL}`, defaultExpected: 'escaped' },
  { name: 'null byte', value: `\0${SQL}`, defaultExpected: 'escaped', nullByte: true },
  { name: 'unicode apostrophe', value: 'ʼ; DROP TABLE t; --', defaultExpected: 'escaped' },
  { name: 'nested triple-quote', value: `'''${SQL}`, defaultExpected: 'escaped' },
  { name: 'multi-line comment', value: "x' /**/; DROP TABLE t; --", defaultExpected: 'escaped' },
  { name: 'newline-based', value: "x'\n; DROP TABLE t\n--", defaultExpected: 'escaped' },
]

// ── Query-building helpers ─────────────────────────────────────

function baseQuery(t: InjectionTarget): Partial<QueryDefinition> {
  return {
    from: t.from,
    ...(t.columns ? { columns: t.columns } : {}),
    ...(t.join ? { joins: [{ table: t.join }] } : {}),
  }
}

function filterDef(t: InjectionTarget, col: ColRef, operator: string, value: unknown): QueryDefinition {
  return {
    ...baseQuery(t),
    filters: [{ column: col.column, ...(col.table ? { table: col.table } : {}), operator, value }],
  } as QueryDefinition
}

function aggQuery(t: InjectionTarget, alias: string): QueryDefinition {
  return {
    ...baseQuery(t),
    columns: [],
    aggregations: [
      {
        column: t.agg.column,
        ...(t.agg.table ? { table: t.agg.table } : {}),
        fn: t.agg.fn,
        alias,
      },
    ],
  } as QueryDefinition
}

// ── describeInjectionContract ──────────────────────────────────

export function describeInjectionContract(
  name: string,
  factory: () => Promise<QueryContract>,
  config: InjectionContractConfig,
): void {
  describe(`InjectionContract: ${name}`, () => {
    let engine: QueryContract

    beforeAll(async () => {
      engine = await factory()
    })

    // ── 16.1 Identifier & Structural Injection ─────────────

    describe('16.1 Identifier & Structural Injection', () => {
      const s = config.structural

      it('column name injection (" payload)', async () => {
        await expectValidationError(
          engine,
          { from: s.table, columns: [`${s.idColumn}"; DROP TABLE ${s.table}; --`] },
          'UNKNOWN_COLUMN',
        )
      })

      it('column name injection (` payload)', async () => {
        await expectValidationError(
          engine,
          { from: s.secondTable, columns: [`${s.idColumn}\`; DROP TABLE ${s.secondTable}; --`] },
          'UNKNOWN_COLUMN',
        )
      })

      it('table name injection', async () => {
        await expectValidationError(engine, { from: `${s.table}; DROP TABLE ${s.table}` }, 'UNKNOWN_TABLE')
      })

      it('EXISTS table name injection', async () => {
        await expectValidationError(
          engine,
          { from: s.table, filters: [{ table: `${s.existsTable}; DROP TABLE ${s.existsTable}` }] },
          'UNKNOWN_TABLE',
        )
      })

      it('column name on cross-DB table', async () => {
        await expectValidationError(
          engine,
          {
            from: s.joinFrom,
            columns: [`${s.idColumn}"; DROP TABLE ${s.joinTo}; --`],
            joins: [{ table: s.joinTo }],
          },
          'UNKNOWN_COLUMN',
        )
      })

      it('ORDER BY direction injection', async () => {
        await expectValidationError(
          engine,
          { from: s.table, orderBy: [{ column: s.idColumn, direction: `asc; DROP TABLE ${s.table};--` as 'asc' }] },
          'INVALID_ORDER_BY',
        )
      })

      it('aggregation function name injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: s.numericColumn, fn: `sum); DROP TABLE ${s.table};--` as 'sum', alias: 'x' }],
          },
          'INVALID_AGGREGATION',
        )
      })

      it('column filter operator injection', async () => {
        await expectValidationError(
          engine,
          { from: s.table, filters: [{ column: s.idColumn, operator: ') OR 1=1 --' as '=', refColumn: s.refColumn }] },
          'INVALID_FILTER',
        )
      })

      it('filter group logic injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            filters: [
              {
                logic: 'and 1=1);--' as 'and',
                conditions: [{ column: s.stringColumn, operator: '=', value: 'active' }],
              },
            ],
          },
          'INVALID_FILTER',
        )
      })

      it('EXISTS count operator injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            filters: [{ table: s.existsTable, count: { operator: ') UNION SELECT 1;--' as '=', value: 1 } }],
          },
          'INVALID_FILTER',
        )
      })

      it('HAVING group logic injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: s.numericColumn, fn: 'sum', alias: 'x' }],
            groupBy: [{ column: s.stringColumn }],
            having: [{ logic: 'or 1=1);--' as 'and', conditions: [{ column: 'x', operator: '>', value: 0 }] }],
          },
          'INVALID_HAVING',
        )
      })

      it('JOIN table name injection', async () => {
        await expectValidationError(
          engine,
          { from: s.table, joins: [{ table: `${s.existsTable}; DROP TABLE ${s.existsTable}` }] },
          'UNKNOWN_TABLE',
        )
      })

      it('ORDER BY column injection', async () => {
        await expectValidationError(
          engine,
          { from: s.table, orderBy: [{ column: `${s.idColumn}"; DROP TABLE ${s.table};--`, direction: 'asc' }] },
          'INVALID_ORDER_BY',
        )
      })

      it('GROUP BY column injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: s.numericColumn, fn: 'sum', alias: 'x' }],
            groupBy: [{ column: `${s.stringColumn}"; DROP TABLE ${s.table};--` }],
          },
          'UNKNOWN_COLUMN',
        )
      })

      it('aggregation column injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: `${s.numericColumn}"; DROP TABLE ${s.table};--`, fn: 'sum', alias: 'x' }],
          },
          'UNKNOWN_COLUMN',
        )
      })

      it('HAVING column (non-alias) injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: s.numericColumn, fn: 'sum', alias: 'x' }],
            groupBy: [{ column: s.stringColumn }],
            having: [{ column: `x"; DROP TABLE ${s.table};--`, operator: '>', value: 0 }],
          },
          'INVALID_HAVING',
        )
      })

      it('HAVING operator injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            columns: [],
            aggregations: [{ column: s.numericColumn, fn: 'sum', alias: 'x' }],
            groupBy: [{ column: s.stringColumn }],
            having: [{ column: 'x', operator: `> 0); DROP TABLE ${s.table};--` as '>', value: 0 }],
          },
          'INVALID_HAVING',
        )
      })

      it('filter value operator injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            filters: [{ column: s.stringColumn, operator: `= 1); DROP TABLE ${s.table};--` as '=', value: 'active' }],
          },
          'INVALID_FILTER',
        )
      })

      it('filter column name injection', async () => {
        await expectValidationError(
          engine,
          {
            from: s.table,
            filters: [{ column: `${s.stringColumn}"; DROP TABLE ${s.table};--`, operator: '=', value: 'active' }],
          },
          'UNKNOWN_COLUMN',
        )
      })
    })

    // ── Per-target tests ───────────────────────────────────────

    for (const target of config.targets) {
      // ── Alias Injection ────────────────────────────────────

      describe(`Alias Injection (${target.label})`, () => {
        it('alias with double-quote injection', async () => {
          await expectInjectionSafe(engine, aggQuery(target, 'x"; DROP TABLE t;--'))
        })

        it('alias with backtick injection', async () => {
          await expectInjectionSafe(engine, aggQuery(target, 'x`; DROP TABLE t;--'))
        })

        it('HAVING referencing injected alias', async () => {
          const alias = 'x"; --'
          await expectInjectionSafe(
            engine,
            {
              ...aggQuery(target, alias),
              groupBy: [{ column: target.agg.groupBy }],
              having: [{ column: alias, operator: '>', value: 0 }],
            } as QueryDefinition,
            'safe',
          )
        })

        it('ORDER BY referencing injected alias', async () => {
          const alias = 'x"; --'
          await expectInjectionSafe(engine, {
            ...aggQuery(target, alias),
            groupBy: [{ column: target.agg.groupBy }],
            orderBy: [{ column: alias, direction: 'asc' }],
          } as QueryDefinition)
        })
      })

      // ── Filter Value Injection ─────────────────────────────

      describe(`Filter Value Injection (${target.label})`, () => {
        for (const { op, value } of STRING_OPS) {
          it(`${op} filter injection`, async () => {
            await expectInjectionSafe(engine, filterDef(target, target.string, op, value))
          })
        }

        it('between injection (string column)', async () => {
          await expectInjectionSafe(engine, filterDef(target, target.string, 'between', { from: SQL, to: 'zzz' }))
        })

        it('notBetween injection (string column)', async () => {
          await expectInjectionSafe(engine, filterDef(target, target.string, 'notBetween', { from: SQL, to: 'zzz' }))
        })

        if (target.numeric) {
          const nc = target.numeric
          it('between injection (numeric column)', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, nc, 'between', { from: `0; DROP TABLE ${target.from}`, to: 100 }),
              'rejected',
            )
          })

          it('notBetween injection (numeric column)', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, nc, 'notBetween', { from: `0; DROP TABLE ${target.from}`, to: 100 }),
              'rejected',
            )
          })
        }

        if (target.timestamp) {
          const tc = target.timestamp
          it('between injection (timestamp column)', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, tc, 'between', { from: `2024-01-01${SQL}`, to: '2024-12-31' }),
              'safe',
            )
          })

          it('notBetween injection (timestamp column)', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, tc, 'notBetween', { from: `2024-01-01${SQL}`, to: '2024-12-31' }),
              'safe',
            )
          })
        }

        if (target.array) {
          const { column: arrCol, expected: arrRawExpected } = target.array
          const arrExpected = arrRawExpected ?? 'escaped'

          it('arrayContains injection', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, { column: arrCol }, 'arrayContains', `x${SQL}`),
              arrExpected,
            )
          })

          it('arrayContainsAll injection', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, { column: arrCol }, 'arrayContainsAll', [`x${SQL}`]),
              arrExpected,
            )
          })

          it('arrayContainsAny injection', async () => {
            await expectInjectionSafe(
              engine,
              filterDef(target, { column: arrCol }, 'arrayContainsAny', [`x${SQL}`]),
              arrExpected,
            )
          })
        }

        if (target.levenshtein) {
          const lev = target.levenshtein
          it('levenshteinLte injection', async () => {
            await expectInjectionSafe(engine, filterDef(target, lev, 'levenshteinLte', { text: SQL, maxDistance: 5 }))
          })
        }

        if (target.byIds) {
          const byIdsExpected = target.byIds
          it('byIds injection', async () => {
            await expectInjectionSafe(engine, { ...baseQuery(target), byIds: [SQL] } as QueryDefinition, byIdsExpected)
          })
        }
      })

      // ── Advanced Injection Vectors ─────────────────────────

      describe(`Advanced Injection Vectors (${target.label})`, () => {
        for (const { name, value, defaultExpected, nullByte } of ADVANCED_PAYLOADS) {
          const expected = nullByte ? (target.nullByteExpected ?? defaultExpected) : defaultExpected

          it(`${name} injection`, async () => {
            await expectInjectionSafe(engine, filterDef(target, target.string, '=', value), expected)
          })
        }

        it('double-quote injection in identifier alias', async () => {
          await expectInjectionSafe(engine, aggQuery(target, '"""; DROP TABLE t;--'))
        })

        it('backtick injection in identifier alias', async () => {
          await expectInjectionSafe(engine, aggQuery(target, '```; DROP TABLE t;--'))
        })
      })
    }
  })
}
