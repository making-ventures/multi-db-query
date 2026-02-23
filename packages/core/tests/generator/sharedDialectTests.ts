/**
 * Shared dialect test runner — tests the same IR inputs against all 3 dialects
 * with per-dialect expected SQL/param assertions.
 *
 * Each DialectTestConfig provides expected output strings for a fixed set of test cases.
 * Tests that are truly unique to one dialect (e.g. Trino catalog, CH named-param typing)
 * stay in their own files.
 */
import { describe, expect, it } from 'vitest'
import type { SqlDialect } from '../../src/dialects/dialect.js'
import type {
  SqlParts,
  WhereArrayCondition,
  WhereBetween,
  WhereColumnCondition,
  WhereCondition,
  WhereCountedSubquery,
  WhereExists,
  WhereFunction,
  WhereGroup,
} from '../../src/types/ir.js'
import { col, makeBase, tbl } from './helpers.js'

// ── Config types ───────────────────────────────────────────────

export interface Expect {
  /** Strings the SQL must contain */
  sql: string[]
  /** Strings the SQL must NOT contain (optional) */
  notSql?: string[]
  /** Expected params array (optional) */
  params?: unknown[]
}

export interface DialectTestConfig {
  /** Display name, e.g. 'PostgresDialect' */
  name: string
  /** Default schema for base(), e.g. 'public' or 'default' */
  schema: string
  /** Child-table schema for subquery tests, e.g. 'public' or 'default' */
  subSchema: string

  // ── SELECT ─────────────────────────────────────────────
  simpleSelect: Expect
  distinct: Expect
  countMode: Expect
  emptySelect: Expect

  // ── JOIN ───────────────────────────────────────────────
  innerJoin: Expect
  leftJoin: Expect

  // ── WHERE standard ─────────────────────────────────────
  equals: Expect
  notEquals: Expect
  lessThan: Expect
  greaterThanOrEqual: Expect

  // ── NULL ───────────────────────────────────────────────
  isNull: Expect
  isNotNull: Expect

  // ── LIKE ───────────────────────────────────────────────
  like: Expect
  notLike: Expect

  // ── Pattern wrapping ───────────────────────────────────
  startsWith: Expect
  endsWith: Expect
  contains: Expect
  notContains: Expect

  // ── Case-insensitive ───────────────────────────────────
  ilike: Expect
  notIlike: Expect
  istartsWith: Expect
  iendsWith: Expect
  icontains: Expect
  notIcontains: Expect

  // ── Wildcard escaping ──────────────────────────────────
  escapesPercent: Expect
  escapesUnderscore: Expect
  escapesBackslash: Expect

  // ── BETWEEN ────────────────────────────────────────────
  between: Expect
  notBetween: Expect

  // ── Levenshtein ────────────────────────────────────────
  levenshtein: Expect

  // ── Array operators ────────────────────────────────────
  arrayContains: Expect
  arrayContainsAll: Expect
  arrayContainsAny: Expect
  arrayIsEmpty: Expect
  arrayIsNotEmpty: Expect

  // ── Column comparison ──────────────────────────────────
  columnComparison: Expect

  // ── Groups ─────────────────────────────────────────────
  orGroup: Expect
  andGroup: Expect
  notGroup: Expect
  singleElementGroup: Expect

  // ── EXISTS ─────────────────────────────────────────────
  exists: Expect
  notExists: Expect
  existsWithSubFilters: Expect

  // ── Counted subquery ───────────────────────────────────
  countedGte: Expect
  countedGt: Expect
  countedLt: Expect
  countedLte: Expect

  // ── GROUP BY + aggregations ────────────────────────────
  groupByCount: Expect
  sumAgg: Expect
  avgMinMax: Expect

  // ── HAVING ─────────────────────────────────────────────
  having: Expect
  havingBetween: Expect
  havingNotBetween: Expect

  // ── ORDER BY ───────────────────────────────────────────
  orderAsc: Expect
  orderDesc: Expect
  orderAggAlias: Expect
  multipleOrder: Expect

  // ── LIMIT / OFFSET ─────────────────────────────────────
  limit: Expect
  offset: Expect
  limitOffset: Expect

  // ── IN / NOT IN ────────────────────────────────────────
  inUuid: Expect
  notInString: Expect
  inInt: Expect
  inDefaultType: Expect
  inSingleElement?: Expect

  // ── Type casts (via IN) ────────────────────────────────
  typeCastDecimal?: Expect
  typeCastBoolean?: Expect
  typeCastDate?: Expect
  typeCastTimestamp?: Expect

  // ── Float param ────────────────────────────────────────
  floatParam?: Expect

  // ── Catalog-qualified ──────────────────────────────────
  catalogTable?: Expect
  catalogJoin?: Expect

  // ── Full query ─────────────────────────────────────────
  fullQuery: Expect
  paramOrder: Expect
}

// ── Runner ─────────────────────────────────────────────────────

export function describeSharedDialectTests(dialect: SqlDialect, cfg: DialectTestConfig): void {
  const base = makeBase(cfg.schema)
  const sub = cfg.subSchema

  function check(parts: SqlParts, params: unknown[], exp: Expect): void {
    const result = dialect.generate(parts, params)
    for (const s of exp.sql) expect(result.sql).toContain(s)
    for (const s of exp.notSql ?? []) expect(result.sql).not.toContain(s)
    if (exp.params !== undefined) expect(result.params).toEqual(exp.params)
  }

  describe(`${cfg.name} — shared dialect tests`, () => {
    // ── SELECT ────────────────────────────────────────────

    describe('SELECT', () => {
      it('simple select', () => check(base(), [], cfg.simpleSelect))
      it('distinct', () => check(base({ distinct: true }), [], cfg.distinct))
      it('count mode', () => check(base({ select: [], countMode: true }), [], cfg.countMode))
      it('empty select → *', () => check(base({ select: [] }), [], cfg.emptySelect))
    })

    // ── JOIN ──────────────────────────────────────────────

    describe('JOIN', () => {
      it('inner join', () => {
        const parts = base({
          joins: [
            { type: 'inner', table: tbl(`${sub}.orders`, 't1'), leftColumn: col('t0', 'id'), rightColumn: col('t1', 'user_id') },
          ],
        })
        check(parts, [], cfg.innerJoin)
      })

      it('left join', () => {
        const parts = base({
          joins: [
            { type: 'left', table: tbl(`${sub}.orders`, 't1'), leftColumn: col('t0', 'id'), rightColumn: col('t1', 'user_id') },
          ],
        })
        check(parts, [], cfg.leftJoin)
      })
    })

    // ── WHERE standard ────────────────────────────────────

    describe('WHERE standard', () => {
      it('equals', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: '=', paramIndex: 0 }
        check(base({ where: cond }), ['Alice'], cfg.equals)
      })

      it('not equals', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: '!=', paramIndex: 0 }
        check(base({ where: cond }), ['Alice'], cfg.notEquals)
      })

      it('less than', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: '<', paramIndex: 0 }
        check(base({ where: cond }), [30], cfg.lessThan)
      })

      it('greater than or equal', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: '>=', paramIndex: 0 }
        check(base({ where: cond }), [18], cfg.greaterThanOrEqual)
      })
    })

    // ── NULL ──────────────────────────────────────────────

    describe('NULL checks', () => {
      it('IS NULL', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNull' }
        check(base({ where: cond }), [], cfg.isNull)
      })

      it('IS NOT NULL', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'isNotNull' }
        check(base({ where: cond }), [], cfg.isNotNull)
      })
    })

    // ── LIKE ──────────────────────────────────────────────

    describe('LIKE', () => {
      it('like raw pattern', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'like', paramIndex: 0 }
        check(base({ where: cond }), ['%foo%'], cfg.like)
      })

      it('notLike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notLike', paramIndex: 0 }
        check(base({ where: cond }), ['%x%'], cfg.notLike)
      })
    })

    // ── Pattern wrapping ──────────────────────────────────

    describe('pattern wrapping', () => {
      it('startsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'startsWith', paramIndex: 0 }
        check(base({ where: cond }), ['Ali'], cfg.startsWith)
      })

      it('endsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'endsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ice'], cfg.endsWith)
      })

      it('contains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['lic'], cfg.contains)
      })

      it('notContains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notContains', paramIndex: 0 }
        check(base({ where: cond }), ['bad'], cfg.notContains)
      })
    })

    // ── Case-insensitive ──────────────────────────────────

    describe('case-insensitive', () => {
      it('ilike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'ilike', paramIndex: 0 }
        check(base({ where: cond }), ['%foo%'], cfg.ilike)
      })

      it('notIlike', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIlike', paramIndex: 0 }
        check(base({ where: cond }), ['%x%'], cfg.notIlike)
      })

      it('istartsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'istartsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ali'], cfg.istartsWith)
      })

      it('iendsWith', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'iendsWith', paramIndex: 0 }
        check(base({ where: cond }), ['ICE'], cfg.iendsWith)
      })

      it('icontains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'icontains', paramIndex: 0 }
        check(base({ where: cond }), ['LIC'], cfg.icontains)
      })

      it('notIcontains', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIcontains', paramIndex: 0 }
        check(base({ where: cond }), ['BAD'], cfg.notIcontains)
      })
    })

    // ── Wildcard escaping ─────────────────────────────────

    describe('wildcard escaping', () => {
      it('escapes %', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['100%'], cfg.escapesPercent)
      })

      it('escapes _', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['a_b'], cfg.escapesUnderscore)
      })

      it('escapes backslash', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'contains', paramIndex: 0 }
        check(base({ where: cond }), ['a\\b'], cfg.escapesBackslash)
      })
    })

    // ── BETWEEN ───────────────────────────────────────────

    describe('BETWEEN', () => {
      it('between', () => {
        const cond: WhereBetween = { column: col('t0', 'age'), fromParamIndex: 0, toParamIndex: 1 }
        check(base({ where: cond }), [18, 65], cfg.between)
      })

      it('not between', () => {
        const cond: WhereBetween = { column: col('t0', 'age'), not: true, fromParamIndex: 0, toParamIndex: 1 }
        check(base({ where: cond }), [0, 17], cfg.notBetween)
      })
    })

    // ── Levenshtein ───────────────────────────────────────

    describe('levenshtein', () => {
      it('levenshtein distance', () => {
        const cond: WhereFunction = {
          fn: 'levenshtein',
          column: col('t0', 'name'),
          fnParamIndex: 0,
          operator: '<=',
          compareParamIndex: 1,
        }
        check(base({ where: cond }), ['test', 2], cfg.levenshtein)
      })
    })

    // ── Array operators ───────────────────────────────────

    describe('array operators', () => {
      it('arrayContains', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'contains', paramIndexes: [0], elementType: 'string' }
        check(base({ where: cond }), ['urgent'], cfg.arrayContains)
      })

      it('arrayContainsAll', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'containsAll', paramIndexes: [0], elementType: 'string' }
        check(base({ where: cond }), [['a', 'b']], cfg.arrayContainsAll)
      })

      it('arrayContainsAny', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'containsAny', paramIndexes: [0], elementType: 'string' }
        check(base({ where: cond }), [['x', 'y']], cfg.arrayContainsAny)
      })

      it('arrayIsEmpty', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isEmpty', elementType: 'string' }
        check(base({ where: cond }), [], cfg.arrayIsEmpty)
      })

      it('arrayIsNotEmpty', () => {
        const cond: WhereArrayCondition = { column: col('t0', 'tags'), operator: 'isNotEmpty', elementType: 'string' }
        check(base({ where: cond }), [], cfg.arrayIsNotEmpty)
      })
    })

    // ── Column comparison ─────────────────────────────────

    describe('column comparison', () => {
      it('column-to-column', () => {
        const cond: WhereColumnCondition = { leftColumn: col('t0', 'a'), operator: '>', rightColumn: col('t1', 'b') }
        check(base({ where: cond }), [], cfg.columnComparison)
      })
    })

    // ── Groups ────────────────────────────────────────────

    describe('groups', () => {
      it('OR group', () => {
        const cond: WhereGroup = {
          logic: 'or',
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), ['Alice', 'Bob'], cfg.orGroup)
      })

      it('AND group', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [
            { column: col('t0', 'age'), operator: '>=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'age'), operator: '<=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), [18, 65], cfg.andGroup)
      })

      it('NOT group', () => {
        const cond: WhereGroup = {
          logic: 'or',
          not: true,
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'name'), operator: '=', paramIndex: 1 } as WhereCondition,
          ],
        }
        check(base({ where: cond }), ['x', 'y'], cfg.notGroup)
      })

      it('single-element group — no parens', () => {
        const cond: WhereGroup = {
          logic: 'and',
          conditions: [{ column: col('t0', 'age'), operator: '>', paramIndex: 0 } as WhereCondition],
        }
        check(base({ where: cond }), [10], cfg.singleElementGroup)
      })
    })

    // ── EXISTS ────────────────────────────────────────────

    describe('EXISTS', () => {
      it('EXISTS', () => {
        const cond: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
        }
        check(base({ where: cond }), [], cfg.exists)
      })

      it('NOT EXISTS', () => {
        const cond: WhereExists = {
          exists: false,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
        }
        check(base({ where: cond }), [], cfg.notExists)
      })

      it('EXISTS with sub-filters', () => {
        const subWhere: WhereCondition = { column: col('s0', 'status'), operator: '=', paramIndex: 0 }
        const cond: WhereExists = {
          exists: true,
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
            where: subWhere,
          },
        }
        check(base({ where: cond }), ['active'], cfg.existsWithSubFilters)
      })
    })

    // ── Counted subquery ──────────────────────────────────

    describe('counted subquery', () => {
      function countedCond(op: string, val: number): { parts: SqlParts; params: unknown[] } {
        const cond: WhereCountedSubquery = {
          subquery: {
            from: tbl(`${sub}.orders`, 's0'),
            join: { leftColumn: col('t0', 'id'), rightColumn: col('s0', 'user_id') },
          },
          operator: op,
          countParamIndex: 0,
        }
        return { parts: base({ where: cond }), params: [val] }
      }

      it('counted >=', () => {
        const { parts, params } = countedCond('>=', 5)
        check(parts, params, cfg.countedGte)
      })

      it('counted >', () => {
        const { parts, params } = countedCond('>', 1)
        check(parts, params, cfg.countedGt)
      })

      it('counted <', () => {
        const { parts, params } = countedCond('<', 2)
        check(parts, params, cfg.countedLt)
      })

      it('counted <=', () => {
        const { parts, params } = countedCond('<=', 1)
        check(parts, params, cfg.countedLte)
      })
    })

    // ── GROUP BY + aggregations ───────────────────────────

    describe('GROUP BY + aggregations', () => {
      it('group by with count', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
        })
        check(parts, [], cfg.groupByCount)
      })

      it('sum', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
        })
        check(parts, [], cfg.sumAgg)
      })

      it('avg, min, max', () => {
        const parts = base({
          select: [],
          from: tbl(`${sub}.orders`, 't0'),
          aggregations: [
            { fn: 'avg', column: col('t0', 'total'), alias: 'avg_amount' },
            { fn: 'min', column: col('t0', 'total'), alias: 'min_amount' },
            { fn: 'max', column: col('t0', 'total'), alias: 'max_amount' },
          ],
        })
        check(parts, [], cfg.avgMinMax)
      })
    })

    // ── HAVING ────────────────────────────────────────────

    describe('HAVING', () => {
      it('simple having', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          having: { column: 'cnt', operator: '>', paramIndex: 0 },
        })
        check(parts, [5], cfg.having)
      })

      it('having between', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
          having: { alias: 'total', fromParamIndex: 0, toParamIndex: 1 },
        })
        check(parts, [100, 1000], cfg.havingBetween)
      })

      it('having not between', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'sum', column: col('t0', 'total'), alias: 'total' }],
          having: { alias: 'total', not: true, fromParamIndex: 0, toParamIndex: 1 },
        })
        check(parts, [100, 1000], cfg.havingNotBetween)
      })
    })

    // ── ORDER BY ──────────────────────────────────────────

    describe('ORDER BY', () => {
      it('asc', () => check(base({ orderBy: [{ column: col('t0', 'name'), direction: 'asc' }] }), [], cfg.orderAsc))
      it('desc', () => check(base({ orderBy: [{ column: col('t0', 'age'), direction: 'desc' }] }), [], cfg.orderDesc))

      it('aggregation alias', () => {
        const parts = base({
          select: [col('t0', 'status')],
          from: tbl(`${sub}.orders`, 't0'),
          groupBy: [col('t0', 'status')],
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          orderBy: [{ column: 'cnt', direction: 'desc' }],
        })
        check(parts, [], cfg.orderAggAlias)
      })

      it('multiple order by', () => {
        const parts = base({
          orderBy: [
            { column: col('t0', 'name'), direction: 'asc' },
            { column: col('t0', 'age'), direction: 'desc' },
          ],
        })
        check(parts, [], cfg.multipleOrder)
      })
    })

    // ── LIMIT / OFFSET ────────────────────────────────────

    describe('LIMIT / OFFSET', () => {
      it('limit', () => check(base({ limit: 10 }), [], cfg.limit))
      it('offset', () => check(base({ offset: 20 }), [], cfg.offset))
      it('limit + offset', () => check(base({ limit: 10, offset: 20 }), [], cfg.limitOffset))
    })

    // ── IN / NOT IN ───────────────────────────────────────

    describe('IN / NOT IN', () => {
      it('in with uuid type', () => {
        const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
        check(base({ where: cond }), [['id1', 'id2']], cfg.inUuid)
      })

      it('notIn with string type', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'notIn', paramIndex: 0, columnType: 'string' }
        check(base({ where: cond }), [['a', 'b']], cfg.notInString)
      })

      it('in with int type', () => {
        const cond: WhereCondition = { column: col('t0', 'age'), operator: 'in', paramIndex: 0, columnType: 'int' }
        check(base({ where: cond }), [[1, 2]], cfg.inInt)
      })

      it('in defaults when type unknown', () => {
        const cond: WhereCondition = { column: col('t0', 'name'), operator: 'in', paramIndex: 0 }
        check(base({ where: cond }), [['a']], cfg.inDefaultType)
      })

      if (cfg.inSingleElement) {
        it('in single element', () => {
          const cond: WhereCondition = { column: col('t0', 'id'), operator: 'in', paramIndex: 0, columnType: 'uuid' }
          check(base({ where: cond }), [['only']], cfg.inSingleElement!)
        })
      }
    })

    // ── Type casts (via IN) ───────────────────────────────

    if (cfg.typeCastDecimal) {
      describe('type casts', () => {
        it('decimal', () => {
          const cond: WhereCondition = { column: col('t0', 'price'), operator: 'in', paramIndex: 0, columnType: 'decimal' }
          check(base({ where: cond }), [[1.5]], cfg.typeCastDecimal!)
        })

        it('boolean', () => {
          const cond: WhereCondition = { column: col('t0', 'active'), operator: 'in', paramIndex: 0, columnType: 'boolean' }
          check(base({ where: cond }), [[true]], cfg.typeCastBoolean!)
        })

        it('date', () => {
          const cond: WhereCondition = { column: col('t0', 'created'), operator: 'in', paramIndex: 0, columnType: 'date' }
          check(base({ where: cond }), [['2024-01-01']], cfg.typeCastDate!)
        })

        it('timestamp', () => {
          const cond: WhereCondition = { column: col('t0', 'ts'), operator: 'in', paramIndex: 0, columnType: 'timestamp' }
          check(base({ where: cond }), [['2024-01-01T00:00:00Z']], cfg.typeCastTimestamp!)
        })
      })
    }

    // ── Float param ───────────────────────────────────────

    if (cfg.floatParam) {
      describe('float param', () => {
        it('float → dialect-specific type', () => {
          const cond: WhereCondition = { column: col('t0', 'score'), operator: '>', paramIndex: 0 }
          check(base({ where: cond }), [3.14], cfg.floatParam!)
        })
      })
    }

    // ── Catalog-qualified ─────────────────────────────────

    if (cfg.catalogTable) {
      describe('catalog-qualified', () => {
        it('catalog-qualified table', () => {
          check(base({ from: tbl(`${cfg.schema}.users`, 't0', 'pg_main') }), [], cfg.catalogTable!)
        })

        it('catalog-qualified join', () => {
          const parts = base({
            joins: [
              { type: 'inner', table: tbl(`${sub}.orders`, 't1', 'pg_main'), leftColumn: col('t0', 'id'), rightColumn: col('t1', 'user_id') },
            ],
          })
          check(parts, [], cfg.catalogJoin!)
        })
      })
    }

    // ── Full query ────────────────────────────────────────

    describe('full query', () => {
      it('complex query combines all clauses', () => {
        const parts: SqlParts = {
          select: [col('t0', 'status')],
          distinct: true,
          from: tbl(`${sub}.orders`, 't0'),
          joins: [
            { type: 'inner', table: tbl(`${sub}.users`, 't1'), leftColumn: col('t0', 'user_id'), rightColumn: col('t1', 'id') },
          ],
          where: { column: col('t1', 'age'), operator: '>=', paramIndex: 0 },
          groupBy: [col('t0', 'status')],
          having: { column: 'cnt', operator: '>', paramIndex: 1 },
          aggregations: [{ fn: 'count', column: '*', alias: 'cnt' }],
          orderBy: [{ column: 'cnt', direction: 'desc' }],
          limit: 5,
          offset: 0,
        }
        check(parts, [18, 2], cfg.fullQuery)
      })

      it('param order preserved', () => {
        const where: WhereGroup = {
          logic: 'and',
          conditions: [
            { column: col('t0', 'name'), operator: '=', paramIndex: 0 } as WhereCondition,
            { column: col('t0', 'age'), operator: '>=', paramIndex: 1 } as WhereCondition,
            { column: col('t0', 'age'), operator: '<=', paramIndex: 2 } as WhereCondition,
          ],
        }
        check(base({ where }), ['Alice', 18, 65], cfg.paramOrder)
      })
    })
  })
}
